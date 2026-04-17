#!/usr/bin/env python3
"""JSONL → Hive-partitioned Parquet.

Layout: {out_dir}/date={YYYY-MM-DD}/source={ats}/part.parquet

Consumers:
  duckdb.sql("SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=1)")
  pd.read_parquet('data/', partitioning='hive')
"""
import argparse, json, sys
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('jsonl')
    ap.add_argument('out_dir')
    ap.add_argument('--date', default=datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                    help='partition date (default today UTC)')
    args = ap.parse_args()

    by_source = defaultdict(list)
    with open(args.jsonl) as f:
        for ln in f:
            r = json.loads(ln)
            by_source[r['source']].append(r)

    for source, rows in by_source.items():
        out = Path(args.out_dir) / f'date={args.date}' / f'source={source}' / 'part.parquet'
        out.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, out, compression='zstd')
        size_mb = out.stat().st_size / 1024 / 1024
        print(f'  {source:<12} {len(rows):>7,} rows → {out} ({size_mb:.1f} MB)')

    total = sum(len(v) for v in by_source.values())
    print(f'total: {total:,} rows in {len(by_source)} partitions')

if __name__ == '__main__':
    main()
