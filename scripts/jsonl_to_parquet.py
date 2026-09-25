#!/usr/bin/env python3
"""JSONL → Hive-partitioned Parquet.

Layout: {out_dir}/date={YYYY-MM-DD}/source={ats}/part-0.parquet

Consumers:
  duckdb.sql("SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=1)")
  pd.read_parquet('data/', partitioning='hive')
"""
import argparse, json, shutil
from pathlib import Path
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq

# Fixed schema: inferring it per file typed all-null columns (e.g. Workday's salary
# fields) as `null`, which clashes with the other sources when read together.
SCHEMA = pa.schema([
    ('id', pa.string()), ('source_slug', pa.string()), ('title', pa.string()),
    ('apply_url', pa.string()), ('description_html', pa.string()),
    ('employment_type', pa.string()), ('department', pa.string()),
    ('locations', pa.list_(pa.string())), ('remote', pa.bool_()),
    ('posted_at', pa.string()), ('updated_at', pa.string()),
    ('salary_min', pa.float64()), ('salary_max', pa.float64()),
    ('salary_currency', pa.string()), ('salary_period', pa.string()),
])
BATCH = 50_000   # rows held in memory per source; a full day is ~1.5M rows / several GB

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('jsonl')
    ap.add_argument('out_dir')
    ap.add_argument('--date', default=datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                    help='partition date (default today UTC)')
    args = ap.parse_args()

    date_dir = Path(args.out_dir) / f'date={args.date}'
    shutil.rmtree(date_dir, ignore_errors=True)

    writers, pending, counts = {}, {}, {}
    def flush(source):
        if source not in writers:
            (date_dir / f'source={source}').mkdir(parents=True, exist_ok=True)
            writers[source] = pq.ParquetWriter(date_dir / f'source={source}' / 'part-0.parquet',
                                               SCHEMA, compression='zstd')
        writers[source].write_table(pa.Table.from_pylist(pending.pop(source), schema=SCHEMA))

    with open(args.jsonl) as f:
        for ln in f:
            r = json.loads(ln)
            source = r.pop('source')
            pending.setdefault(source, []).append(r)
            counts[source] = counts.get(source, 0) + 1
            if len(pending[source]) >= BATCH:
                flush(source)
    for source in list(pending):
        flush(source)
    for w in writers.values():
        w.close()

    for source, n in counts.items():
        out = date_dir / f'source={source}'
        size_mb = sum(p.stat().st_size for p in out.glob('*.parquet')) / 1024 / 1024
        print(f'  {source:<16} {n:>9,} rows → {out}/ ({size_mb:.1f} MB)')
    print(f'total: {sum(counts.values()):,} rows in {len(counts)} partitions')

if __name__ == '__main__':
    main()
