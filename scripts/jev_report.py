#!/usr/bin/env python3
"""Compare what happened to jobs Jev would set aside vs. the other ambiguous jobs.

Reads the jev_seniority / jev_confidence columns that `build_shortlist.py --jev`
writes (shadow or enforce mode) across every shortlist date, joins them with
applications/status.jsonl, and prints one row per group.

Reply outcomes (rejected, interview, ...) are not tracked anywhere yet. If
applications/outcomes.jsonl exists with {"id": ..., "outcome": ...} records, they
are counted too; until then the report can only show application status.

Example:
  python scripts/jev_report.py --threshold 0.9
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def scored_rows(shortlist_dir: Path) -> dict[str, dict]:
    """Latest Jev answer per job id across all shortlist dates."""
    rows: dict[str, dict] = {}
    for path in sorted(shortlist_dir.glob('date=*/*.csv')):
        if path.stem not in ('fresh', 'backlog', 'removed'):
            continue
        with path.open(newline='', encoding='ascii') as f:
            for row in csv.DictReader(f):
                if row.get('jev_confidence'):
                    rows[row['id']] = row
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--shortlist-dir', default='shortlists')
    parser.add_argument('--status', default='applications/status.jsonl')
    parser.add_argument('--outcomes', default='applications/outcomes.jsonl')
    parser.add_argument('--threshold', type=float, default=0.9)
    args = parser.parse_args()

    rows = scored_rows(Path(args.shortlist_dir))
    if not rows:
        raise SystemExit(f'No Jev-scored rows under {args.shortlist_dir}; run build_shortlist.py --jev first')
    # Later records win, matching how apply.py reads the ledger.
    statuses = {r['id']: r.get('status', '') for r in read_jsonl(Path(args.status))}
    outcomes = {r['id']: r.get('outcome', '') for r in read_jsonl(Path(args.outcomes))}

    groups: dict[str, list[str]] = {'jev would set aside': [], 'other ambiguous': []}
    for job_id, row in rows.items():
        flagged = row['jev_seniority'] == 'senior' and float(row['jev_confidence']) >= args.threshold
        groups['jev would set aside' if flagged else 'other ambiguous'].append(job_id)

    status_names = ['submitted', 'blocked', 'needs_input', 'unknown']
    print(f'Jev-scored jobs: {len(rows)}  (threshold {args.threshold})\n')
    print(f"{'group':<22}{'jobs':>7}{'not tried':>11}" + ''.join(f'{s:>13}' for s in status_names))
    for name, ids in groups.items():
        counts = Counter(statuses.get(i, '') for i in ids)
        print(f'{name:<22}{len(ids):>7}{counts[""]:>11}' + ''.join(f'{counts[s]:>13}' for s in status_names))

    if outcomes:
        print('\nReply outcomes (submitted applications only):')
        for name, ids in groups.items():
            submitted = [i for i in ids if statuses.get(i) == 'submitted']
            counts = Counter(outcomes.get(i) or 'no record' for i in submitted)
            detail = ', '.join(f'{k} {v} ({v / len(submitted):.0%})' for k, v in counts.most_common()) if submitted else 'none submitted'
            print(f'  {name}: {detail}')
    else:
        print(f'\nNo reply outcomes yet ({args.outcomes} missing), so this cannot show whether '
              'flagged jobs get rejected more often.')


if __name__ == '__main__':
    main()
