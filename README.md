# open-apply

An open dataset of active job postings, refreshed daily from public ATS APIs.

- **Dataset:** https://huggingface.co/datasets/edwarddgao/open-apply-jobs (see HF for current row counts)
- **Sources:** Greenhouse, Lever, Ashby
- **Refresh:** daily at 06:00 UTC via GitHub Actions

## Demo

Powers an automated job-application agent that fills ATS forms via Claude-Code-spawned subagents.

[![Demo video](https://img.youtube.com/vi/MiDGBzrxSNk/maxresdefault.jpg)](https://youtu.be/MiDGBzrxSNk?si=C9HVTmRYluw_w2gM)

## Pipeline

```
slugs/cc_{ats}_FINAL.txt  →  oa_adapter.py  →  jobs.jsonl
                                                    │
                                                    ▼
                                      scripts/jsonl_to_parquet.py
                                                    │
                                                    ▼
                                      data/date=YYYY-MM-DD/source={ats}/
                                                    │
                                                    ▼
                                      scripts/publish_hf.py
                                                    │
                                                    ▼
                               huggingface.co/datasets/...
```

1. **Adapters** fan out across ~12k tenant slugs via `ThreadPoolExecutor`, hitting the public JSON endpoint for each ATS.
2. **Conversion** groups records into Hive-partitioned Parquet (`date=/source=`) via `pq.write_to_dataset`.
3. **Publish** uploads the day's partition folder to HuggingFace Datasets, preserving previous days as a time-series.

## Sources

| ATS | Endpoint pattern |
|---|---|
| Greenhouse | `https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true` |
| Lever | `https://api.lever.co/v0/postings/{slug}?mode=json` |
| Ashby | `https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true` |

All three are public, unauthenticated JSON APIs. No ToS violation.

## Slug discovery

Tenant slugs are harvested via Common Crawl CDX queries against `boards.greenhouse.io/*`, `jobs.lever.co/*`, `jobs.ashbyhq.com/*`, unioned across 5 recent crawl snapshots (~10k distinct tenants). Monthly re-runs catch new tenants organically — slugs are stable and companies rarely renumber their boards.

## Schema

Records follow a subset of [schema.org JobPosting](https://schema.org/JobPosting):

| Field | Type | Notes |
|---|---|---|
| `id` | str | `{source}:{slug}:{native_id}` |
| `source` | str | `greenhouse` \| `lever` \| `ashby` |
| `source_slug` | str | tenant slug on that ATS |
| `title` | str | |
| `apply_url` | str | canonical apply link |
| `description_html` | str? | ~100% populated |
| `employment_type` | str? | e.g. `FullTime`, `Contract` |
| `department` | str? | |
| `locations` | list[str] | |
| `remote` | bool? | inferred from location text if not structured |
| `posted_at` | ISO 8601? | Original publish/create timestamp where exposed (`first_published` for Greenhouse, `createdAt` for Lever, `publishedAt` for Ashby) |
| `updated_at` | ISO 8601? | Last updated timestamp where exposed (currently Greenhouse; sometimes absent for Lever/Ashby public APIs) |
| `salary_min` / `salary_max` | float? | 25–35% populated (Ashby/Lever expose structured comp; Greenhouse embeds in HTML) |
| `salary_currency` / `salary_period` | str? | |

### Timestamp semantics

Partitions published before the timestamp migration used a best-effort `posted_at` field. For Greenhouse rows in those historical partitions, `posted_at` was populated from `updated_at` when present, so it may represent the last modification time rather than the original publication time. New partitions populate `posted_at` from the original publish/create field and expose `updated_at` separately when the ATS provides it.

## Local run

```bash
pip install -r requirements.txt
python oa_adapter.py --workers 16 --out jobs.jsonl         # ~15 min
python scripts/jsonl_to_parquet.py jobs.jsonl data         # partition
python scripts/build_shortlist.py --data-dir data --date latest --out-dir shortlists
HF_TOKEN=... python scripts/publish_hf.py data             # upload
```

## Load the published data

```python
from datasets import load_dataset
ds = load_dataset('edwarddgao/open-apply-jobs')

# or with DuckDB
import duckdb
duckdb.sql("""
  SELECT source, COUNT(*) FROM read_parquet(
    'hf://datasets/edwarddgao/open-apply-jobs/data/**/*.parquet',
    hive_partitioning=1
  )
  WHERE date = '2026-04-17'
  GROUP BY source
""")
```

## License

- **Code:** MIT
- **Data:** Public job postings from public ATS APIs; the underlying listings belong to the companies that posted them. Redistributed as a convenience; use responsibly.
