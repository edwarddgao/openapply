#!/usr/bin/env python3
"""JSONL → Hive-partitioned Parquet.

Layout: {out_dir}/date={YYYY-MM-DD}/source={ats}/part.parquet

Consumers:
  duckdb.sql("SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=1)")
  pd.read_parquet('data/', partitioning='hive')
"""
import argparse, json
from pathlib import Path
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('jsonl')
    ap.add_argument('out_dir')
    ap.add_argument('--date', default=datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                    help='partition date (default today UTC)')
    args = ap.parse_args()

    with open(args.jsonl) as f:
        rows = [json.loads(ln) for ln in f]
    for r in rows:
        r['date'] = args.date

    table = pa.Table.from_pylist(rows)
    pq.write_to_dataset(
        table, root_path=args.out_dir,
        partition_cols=['date', 'source'],
        compression='zstd',
        existing_data_behavior='overwrite_or_ignore',
    )

    by_source = table.group_by('source').aggregate([([], 'count_all')])
    for source, n in zip(by_source['source'].to_pylist(), by_source['count_all'].to_pylist()):
        out = Path(args.out_dir) / f'date={args.date}' / f'source={source}'
        size_mb = sum(p.stat().st_size for p in out.glob('*.parquet')) / 1024 / 1024
        print(f'  {source:<12} {n:>7,} rows → {out}/ ({size_mb:.1f} MB)')
    print(f'total: {len(rows):,} rows in {len(by_source)} partitions')

if __name__ == '__main__':
    main()
