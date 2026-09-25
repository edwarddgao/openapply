#!/usr/bin/env python3
"""Evaluate TypeSafe Jev (and Claude Haiku 4.5) on shortlist rows the regexes
cannot place on seniority (seniority_bucket == 'ambiguous-no-years').

Workflow:
  1. python scripts/eval_jev_seniority.py sample --n 200
       writes evals/jev_seniority/date=D/{states.jsonl,labels.csv}
  2. hand-fill the `label` column in labels.csv with entry / mid / senior
  3. python scripts/eval_jev_seniority.py run --model jev     (OPENROUTER_API_KEY or TYPESAFE_API_KEY)
     python scripts/eval_jev_seniority.py run --model haiku   (ANTHROPIC_API_KEY or `ant auth login`)
  4. python scripts/eval_jev_seniority.py score

Both models see the same state: title, department, and a requirements-focused
excerpt of the description. Jev's docs warn that accuracy falls as state fills
with unrelated detail, so the excerpt is deliberate, not a size workaround.

The shortlist currently keeps every ambiguous row, which is equivalent to
predicting "not senior" for all of them; `score` reports that as the baseline.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.build_shortlist import clean, latest_date, strip_html  # noqa: E402
from scripts.jev import CRITERIA, LABELS, Jev, job_state, load_dotenv  # noqa: E402


EVAL_DIR = Path('evals/jev_seniority')

HAIKU_MODEL = 'claude-haiku-4-5'
HAIKU_SYSTEM = (
    'You classify job postings by the seniority level they hire for. '
    'Choose exactly one level:\n' + '\n'.join(f'- {k}: {v}' for k, v in CRITERIA.items())
)


def descriptions_by_id(data_dir: Path, snapshot_date: str, ids: set[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    for path in sorted((data_dir / f'date={snapshot_date}').glob('source=*/*.parquet')):
        table = pq.read_table(path, columns=['id', 'description_html'])
        for row in table.to_pylist():
            if row['id'] in ids:
                found[row['id']] = strip_html(row['description_html'])
    return found


def cmd_sample(args: argparse.Namespace) -> None:
    snapshot_date = latest_date(Path(args.data_dir)) if args.date == 'latest' else args.date
    fresh = Path(args.shortlist_dir) / f'date={snapshot_date}' / 'fresh.csv'
    with fresh.open(newline='', encoding='utf-8') as f:
        rows = [r for r in csv.DictReader(f) if r['seniority_bucket'] == 'ambiguous-no-years']
    rows = random.Random(args.seed).sample(rows, min(args.n, len(rows)))
    descriptions = descriptions_by_id(Path(args.data_dir), snapshot_date, {r['id'] for r in rows})

    out = EVAL_DIR / f'date={snapshot_date}'
    out.mkdir(parents=True, exist_ok=True)
    labels_path = out / 'labels.csv'
    if labels_path.exists() and not args.force:
        raise SystemExit(f'{labels_path} exists; pass --force to overwrite (this discards hand labels)')
    with (out / 'states.jsonl').open('w', encoding='utf-8') as states, labels_path.open('w', newline='', encoding='utf-8') as labels:
        writer = csv.writer(labels)
        writer.writerow(['id', 'company_slug', 'title', 'role_type', 'apply_url', 'label', 'notes', 'excerpt'])
        for row in rows:
            state = job_state(row['title'], clean(row['department']), descriptions.get(row['id'], row['snippet']))
            states.write(json.dumps({'id': row['id'], 'state': state}) + '\n')
            writer.writerow([row['id'], row['company_slug'], row['title'], row['role_type'], row['apply_url'], '', '', state['requirements_excerpt'][:600]])
    print(f'Sampled {len(rows)} of the ambiguous rows -> {labels_path}')
    print(f'Fill `label` with one of {", ".join(LABELS)}, then run `run --model jev` / `run --model haiku`.')


def ask_haiku(client: Any, state: dict[str, Any]) -> dict[str, Any]:
    response = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=256,
        system=HAIKU_SYSTEM,
        messages=[{'role': 'user', 'content': json.dumps({'job': state}, indent=1)}],
        output_config={
            'format': {
                'type': 'json_schema',
                'schema': {
                    'type': 'object',
                    'properties': {'seniority': {'type': 'string', 'enum': list(LABELS)}},
                    'required': ['seniority'],
                    'additionalProperties': False,
                },
            }
        },
    )
    text = next(b.text for b in response.content if b.type == 'text')
    return {'label': json.loads(text)['seniority'], 'confidence': None, 'probabilities': None}


def make_predictor(model: str):
    load_dotenv()
    if model == 'jev':
        try:
            return Jev().seniority
        except RuntimeError as exc:
            raise SystemExit(str(exc))
    try:
        import anthropic
    except ImportError:
        raise SystemExit('pip install anthropic to run the Haiku baseline')
    client = anthropic.Anthropic(max_retries=5)
    return lambda state: ask_haiku(client, state)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def eval_dir(date_arg: str) -> Path:
    if date_arg != 'latest':
        return EVAL_DIR / f'date={date_arg}'
    dirs = sorted(EVAL_DIR.glob('date=*'))
    if not dirs:
        raise SystemExit(f'No samples under {EVAL_DIR}; run `sample` first')
    return dirs[-1]


def cmd_run(args: argparse.Namespace) -> None:
    out = eval_dir(args.date)
    pred_path = out / f'predictions-{args.model}.jsonl'
    done = {p['id'] for p in read_jsonl(pred_path)}
    todo = [s for s in read_jsonl(out / 'states.jsonl') if s['id'] not in done]
    predict = make_predictor(args.model)

    def one(item: dict[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        try:
            result = predict(item['state'])
        except Exception as exc:  # recorded and skipped by `score`; rerun `run` to retry
            return {'id': item['id'], 'error': repr(exc)}
        return {'id': item['id'], **result, 'latency_s': round(time.monotonic() - started, 3)}

    with ThreadPoolExecutor(args.workers) as pool, pred_path.open('a', encoding='utf-8') as f:
        for index, prediction in enumerate(pool.map(one, todo), 1):
            if 'error' in prediction:
                print(f"error {prediction['id']}: {prediction['error']}", file=sys.stderr)
                continue
            f.write(json.dumps(prediction) + '\n')
            f.flush()
            if index % 25 == 0:
                print(f'{index}/{len(todo)}')
    print(f'{args.model}: {len(read_jsonl(pred_path))} predictions in {pred_path}')


def is_senior(label: str) -> bool:
    return label == 'senior'


def report(name: str, pairs: list[tuple[str, str]]) -> None:
    n = len(pairs)
    if not n:
        print(f'{name}: no labeled predictions')
        return
    exact = sum(p == g for g, p in pairs) / n
    tp = sum(is_senior(g) and is_senior(p) for g, p in pairs)
    fp = sum(not is_senior(g) and is_senior(p) for g, p in pairs)
    fn = sum(is_senior(g) and not is_senior(p) for g, p in pairs)
    binary = sum(is_senior(g) == is_senior(p) for g, p in pairs) / n
    precision = tp / (tp + fp) if tp + fp else float('nan')
    recall = tp / (tp + fn) if tp + fn else float('nan')
    print(f'{name:<24} n={n:<4} 3-way acc={exact:.1%}  senior-vs-not acc={binary:.1%}  '
          f'senior precision={precision:.1%} recall={recall:.1%}  wrongly-dropped={fp}')


def cmd_score(args: argparse.Namespace) -> None:
    out = eval_dir(args.date)
    with (out / 'labels.csv').open(newline='', encoding='utf-8') as f:
        gold = {r['id']: r['label'].strip().lower() for r in csv.DictReader(f) if r['label'].strip()}
    bad = {v for v in gold.values() if v not in LABELS}
    if bad:
        raise SystemExit(f'Unknown labels in labels.csv: {sorted(bad)}')
    if not gold:
        raise SystemExit('No labels yet; fill the `label` column in labels.csv')
    counts = {label: sum(v == label for v in gold.values()) for label in LABELS}
    print(f'{len(gold)} labeled rows: {counts}\n')

    # Current pipeline: ambiguous rows stay in the shortlist, i.e. "not senior".
    report('regex (keep all)', [(g, 'mid') for g in gold.values()])
    for model in ('jev', 'haiku'):
        preds = {p['id']: p for p in read_jsonl(out / f'predictions-{model}.jsonl')}
        pairs = [(gold[i], preds[i]['label']) for i in gold if i in preds]
        report(model, pairs)
        if model == 'jev' and pairs:
            latencies = sorted(preds[i]['latency_s'] for i in gold if i in preds)
            print(f"{'':<24} p50 latency={latencies[len(latencies) // 2]:.2f}s")
            for threshold in (0.5, 0.7, 0.9):
                # Only act on confident answers; everything below the bar falls back to keep.
                gated = [(gold[i], preds[i]['label'] if preds[i]['confidence'] >= threshold else 'mid') for i in gold if i in preds]
                acted = sum(preds[i]['confidence'] >= threshold for i in gold if i in preds)
                report(f'jev conf>={threshold} ({acted} acted)', gated)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    sample = sub.add_parser('sample', help='sample ambiguous rows for hand labeling')
    sample.add_argument('--data-dir', default='data')
    sample.add_argument('--shortlist-dir', default='shortlists')
    sample.add_argument('--date', default='latest')
    sample.add_argument('--n', type=int, default=200)
    sample.add_argument('--seed', type=int, default=0)
    sample.add_argument('--force', action='store_true')
    run = sub.add_parser('run', help='get predictions for sampled rows (resumable)')
    run.add_argument('--model', choices=('jev', 'haiku'), required=True)
    run.add_argument('--date', default='latest')
    run.add_argument('--workers', type=int, default=8)
    score = sub.add_parser('score', help='compare predictions against hand labels')
    score.add_argument('--date', default='latest')
    args = parser.parse_args()
    {'sample': cmd_sample, 'run': cmd_run, 'score': cmd_score}[args.command](args)


if __name__ == '__main__':
    main()
