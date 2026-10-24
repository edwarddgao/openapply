#!/usr/bin/env python3
"""Per-run API cost and outcome report for overnight application runs.

Joins each Claude Code role-agent transcript (~/.claude/projects/<repo>/*.jsonl) to the
application ledger by role id, prices usage from the published per-model table, and
prints one summary line per run plus optional per-role rows.

    python3 analysis/overnight_cost.py 2026-09-27 2026-09-28          # compare runs
    python3 analysis/overnight_cost.py 2026-09-29 --roles              # per-role detail
    python3 analysis/overnight_cost.py latest                          # latest-summary.json

Cost is computed here rather than taken from the transcript's `cost-state` record: the
CLI logs `unrecognized_model` for ids its alias table lacks (claude-sonnet-5-5 on 2.1.283)
and its own figure is then unreliable. Both numbers are shown when they differ.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TRANSCRIPTS = Path.home() / '.claude' / 'projects' / ('-' + str(REPO).strip('/').replace('/', '-'))
LEDGER = REPO / 'applications' / 'status.jsonl'
OVERNIGHT = REPO / 'applications' / 'overnight'

# $ per million tokens: input, 5m cache write, 1h cache write, cache read, output
# (platform.claude.com/docs/en/about-claude/pricing, fetched 2026-09-28)
PRICES = {
    # Haiku 5.5 is tiered by prompt length; the >100k-token tier is applied per request
    # in price() (HAIKU_55_LONG). Every overnight role-agent prompt measured so far is
    # under 75k tokens, so the base tier is the one that applies in practice.
    'claude-haiku-5-5': (0.10, 0.125, 0.20, 0.01, 0.50),
    'claude-sonnet-5-5': (2.0, 2.5, 4.0, 0.20, 10.0),
    'claude-sonnet-5': (2.0, 2.5, 4.0, 0.20, 10.0),
    'claude-opus-5-5': (4.0, 5.0, 8.0, 0.20, 20.0),
    'claude-opus-5': (5.0, 6.25, 10.0, 0.50, 25.0),
    'claude-haiku-4-5': (1.0, 1.25, 2.0, 0.10, 5.0),
}
HAIKU_55_LONG = (0.50, 0.625, 1.0, 0.05, 2.50)  # Claude Haiku 5.5, prompts over 100k tokens
ROLE_ID_RE = re.compile(r'^- ID: (\S+)', re.M)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def price(model: str, usage: dict) -> float | None:
    rates = PRICES.get(model)
    if not rates:
        return None
    prompt_tokens = (usage.get('input_tokens', 0) + usage.get('cache_read_input_tokens', 0)
                     + usage.get('cache_creation_input_tokens', 0))
    if model == 'claude-haiku-5-5' and prompt_tokens > 100_000:
        rates = HAIKU_55_LONG
    creation = usage.get('cache_creation') or {}
    w5 = creation.get('ephemeral_5m_input_tokens')
    w1 = creation.get('ephemeral_1h_input_tokens')
    if w5 is None and w1 is None:
        w5, w1 = usage.get('cache_creation_input_tokens', 0), 0
    tokens = (usage.get('input_tokens', 0), w5 or 0, w1 or 0,
              usage.get('cache_read_input_tokens', 0), usage.get('output_tokens', 0))
    return sum(t * r for t, r in zip(tokens, rates)) / 1e6


def load_session(path: Path) -> dict | None:
    first_prompt = None
    requests: dict[str, tuple[str, dict, str]] = {}
    cost_state = None
    effort = None
    first_ts = last_ts = None
    with path.open(encoding='utf-8') as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = row.get('type')
            if kind == 'user' and first_prompt is None:
                content = row.get('message', {}).get('content')
                first_prompt = content if isinstance(content, str) else json.dumps(content)
                if not first_prompt.startswith('You are an external job application agent'):
                    return None
                first_ts = row.get('timestamp')
            elif kind == 'assistant':
                msg = row.get('message', {})
                rid = row.get('requestId') or row.get('uuid')
                if rid not in requests and msg.get('usage'):
                    requests[rid] = (msg.get('model', ''), msg['usage'], row.get('timestamp', ''))
                effort = effort or row.get('effort')
                last_ts = row.get('timestamp') or last_ts
            elif kind == 'cost-state':
                cost_state = row
    if first_prompt is None or not requests:
        return None
    match = ROLE_ID_RE.search(first_prompt)
    models = Counter(m for m, _, _ in requests.values())
    model = models.most_common(1)[0][0]
    per_model_cost = defaultdict(float)
    unknown = False
    totals = Counter()
    for m, usage, _ in requests.values():
        c = price(m, usage)
        if c is None:
            unknown = True
        else:
            per_model_cost[m] += c
        totals['in'] += usage.get('input_tokens', 0)
        totals['write'] += usage.get('cache_creation_input_tokens', 0)
        totals['read'] += usage.get('cache_read_input_tokens', 0)
        totals['out'] += usage.get('output_tokens', 0)
        totals['think'] += (usage.get('output_tokens_details') or {}).get('thinking_tokens', 0)
    seconds = None
    if first_ts and last_ts:
        seconds = (parse_ts(last_ts) - parse_ts(first_ts)).total_seconds()
    return {
        'session': path.stem,
        'role_id': match.group(1) if match else None,
        'model': model,
        'effort': effort,
        'turns': len(requests),
        'cost': None if unknown else sum(per_model_cost.values()),
        'cli_cost': (cost_state or {}).get('totalCostUSD'),
        'cli_unknown': (cost_state or {}).get('hasUnknownModelCost'),
        'tokens': dict(totals),
        'seconds': seconds,
        'started': first_ts,
        'ended': last_ts,
    }


def resolve_run(token: str) -> Path:
    if token == 'latest':
        summary = json.loads((OVERNIGHT / 'latest-summary.json').read_text())
        return REPO / Path(summary['run_log']).parent
    p = Path(token)
    if p.is_dir():
        return p
    runs = sorted((OVERNIGHT / f'date={token}').glob('run=*'))
    if not runs:
        sys.exit(f'no run directory for {token}')
    completed = [r for r in runs if (r / 'summary.json').exists()
                 and json.loads((r / 'summary.json').read_text()).get('applications')]
    return (completed or runs)[-1]


def apply_window(run_dir: Path) -> tuple[datetime, datetime | None, dict]:
    summary = json.loads((run_dir / 'summary.json').read_text())
    apply_stages = [s for s in summary.get('stages', []) if s['name'].startswith('apply to selected roles')]
    if apply_stages:
        start = parse_ts(apply_stages[0]['started_at'])
        ends = [s.get('finished_at') for s in apply_stages if s.get('finished_at')]
        end = parse_ts(max(ends)) if ends and summary.get('finished_at') else None
    else:
        start = parse_ts(summary['started_at'])
        end = parse_ts(summary['finished_at']) if summary.get('finished_at') else None
    return start, end, summary


def ledger_rows(start: datetime, end: datetime | None) -> dict[str, dict]:
    rows = {}
    if not LEDGER.exists():
        return rows
    for line in LEDGER.read_text(encoding='utf-8').splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = row.get('started_at')
        if not ts:
            continue
        t = parse_ts(ts)
        if t >= start and (end is None or t <= end):
            rows[row['id']] = row
    return rows


def sessions_in(start: datetime, end: datetime | None) -> list[dict]:
    out = []
    if not TRANSCRIPTS.is_dir():
        sys.exit(f'transcript dir missing: {TRANSCRIPTS}')
    lo = start.timestamp() - 60
    for path in TRANSCRIPTS.glob('*.jsonl'):
        if path.stat().st_mtime < lo:
            continue
        s = load_session(path)
        if not s or not s['started']:
            continue
        t = parse_ts(s['started'])
        if t < start or (end is not None and t > end):
            continue
        out.append(s)
    out.sort(key=lambda s: s['started'])
    return out


def fmt_money(v: float | None) -> str:
    return '   n/a' if v is None else f'{v:6.3f}'


def report(run_dir: Path, show_roles: bool) -> dict:
    start, end, summary = apply_window(run_dir)
    sessions = sessions_in(start, end)
    ledger = ledger_rows(start, end)
    for s in sessions:
        s['status'] = (ledger.get(s['role_id']) or {}).get('status', '?')
    models = Counter(s['model'] for s in sessions)
    efforts = Counter(str(s['effort']) for s in sessions)
    priced = [s for s in sessions if s['cost'] is not None]
    total = sum(s['cost'] for s in priced)
    cli_total = sum(s['cli_cost'] or 0 for s in sessions)
    statuses = Counter(s['status'] for s in sessions)
    submitted = statuses.get('submitted', 0)
    secs = [s['seconds'] for s in sessions if s['seconds'] is not None]
    turns = [s['turns'] for s in sessions]
    label = run_dir.parent.name.split('=')[1]
    state = summary.get('outcome', 'running')
    print(f'run {label} [{state}]  window {start:%m-%d %H:%MZ} -> '
          f'{end.strftime("%H:%MZ") if end else "open"}  '
          f'model={dict(models)} effort={dict(efforts)}')
    print(f'  sessions={len(sessions)} ledger_rows={len(ledger)} status={dict(sorted(statuses.items()))}')
    if sessions:
        print(f'  api$ total={total:.2f} mean/role={total / max(len(priced), 1):.3f} '
              f'per_submitted={total / submitted if submitted else float("nan"):.3f}  '
              f'(cli cost-state total={cli_total:.2f})')
        print(f'  mean turns={sum(turns) / len(turns):.1f}  mean sec={sum(secs) / max(len(secs), 1):.0f}  '
              f'tokens: read={sum(s["tokens"]["read"] for s in sessions):,} '
              f'write={sum(s["tokens"]["write"] for s in sessions):,} '
              f'out={sum(s["tokens"]["out"] for s in sessions):,} '
              f'think={sum(s["tokens"]["think"] for s in sessions):,}')
        by_status = defaultdict(list)
        for s in priced:
            by_status[s['status']].append(s['cost'])
        print('  mean $ by status: ' + ', '.join(
            f'{k}={sum(v) / len(v):.3f} (n={len(v)})' for k, v in sorted(by_status.items())))
    if show_roles:
        print(f'  {"role":58} {"status":11} {"model":17} turns  api$   cli$    sec')
        for s in sessions:
            print(f'  {(s["role_id"] or s["session"])[:58]:58} {s["status"]:11} {s["model"]:17} '
                  f'{s["turns"]:5d} {fmt_money(s["cost"])} {fmt_money(s["cli_cost"])} '
                  f'{s["seconds"] or 0:6.0f}')
    return {'run': label, 'sessions': len(sessions), 'total': total, 'submitted': submitted,
            'models': dict(models), 'statuses': dict(statuses)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('runs', nargs='+', help='YYYY-MM-DD, run directory, or "latest"')
    ap.add_argument('--roles', action='store_true', help='print per-role rows')
    args = ap.parse_args()
    for token in args.runs:
        report(resolve_run(token), args.roles)
        print()


if __name__ == '__main__':
    main()
