---
license: mit
language:
  - en
task_categories:
  - text-classification
  - text-retrieval
tags:
  - jobs
  - hiring
  - greenhouse
  - lever
  - ashby
  - ats
size_categories:
  - 100K<n<1M
pretty_name: Open-Apply Jobs
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/**/*.parquet
---

# Open-Apply Jobs

A daily-refreshed open dataset of active job postings sourced directly from public ATS APIs (Greenhouse, Lever, Ashby). Every record can be traced back to the hiring company's own career board.

- **Refresh:** automated daily at 06:00 UTC
- **Partitioning:** Hive-partitioned Parquet (`date=YYYY-MM-DD/source={ats}`)
- **Source code:** https://github.com/edwarddgao/openapply

## Usage

```python
from datasets import load_dataset
ds = load_dataset('edwarddgao/open-apply-jobs')
```

```python
# Latest snapshot only
import duckdb
duckdb.sql("""
  SELECT title, source_slug, apply_url, locations
  FROM read_parquet('hf://datasets/edwarddgao/open-apply-jobs/data/**/*.parquet',
                    hive_partitioning=1)
  WHERE date = (SELECT MAX(date) FROM read_parquet(
    'hf://datasets/edwarddgao/open-apply-jobs/data/**/*.parquet', hive_partitioning=1))
    AND 'Software Engineer' = ANY(string_split(title, ' '))
""").show()
```

## Partition layout

```
data/
├── date=YYYY-MM-DD/
│   ├── source=greenhouse/part-*.parquet
│   ├── source=lever/part-*.parquet
│   └── source=ashby/part-*.parquet
├── date=YYYY-MM-DD/
│   └── ...
```

Each `date=` folder is a full snapshot of that day's active postings — not an incremental delta. Diff two consecutive dates to get added / removed / modified jobs.

## Schema

| Field | Type | Notes |
|---|---|---|
| `id` | string | `{source}:{slug}:{native_id}` — unique across the dataset |
| `source` | string | `greenhouse` \| `lever` \| `ashby` |
| `source_slug` | string | Tenant slug on that ATS (e.g. `databricks`, `spacex`) |
| `title` | string | Job title |
| `apply_url` | string | Canonical URL on the ATS career site |
| `description_html` | string? | Full HTML description (~100% populated) |
| `employment_type` | string? | e.g. `FullTime`, `Contract`, `Internship` |
| `department` | string? | Free-form; ATS-dependent |
| `locations` | list[string] | Always a list; may be empty for fully-remote |
| `remote` | bool? | Structured where available; otherwise inferred from location text |
| `posted_at` | string? | ISO 8601 UTC |
| `salary_min` / `salary_max` | float? | Only Ashby/Lever expose structured comp (~30%). Greenhouse salary is embedded in `description_html` |
| `salary_currency` | string? | ISO 4217 (`USD`, `EUR`, `GBP`) |
| `salary_period` | string? | `HOUR` \| `DAY` \| `WEEK` \| `MONTH` \| `YEAR` |

## Collection methodology

1. **Tenant discovery.** Common Crawl CDX queries against `boards.greenhouse.io/*`, `jobs.lever.co/*`, `jobs.ashbyhq.com/*`, unioned across five crawl snapshots. ~10k distinct tenants per refresh.
2. **Fetch.** Each tenant's public JSON board API is called with 16-way concurrency; retries only on transient 5xx/408/429/network errors.
3. **Normalize.** Per-ATS records are mapped to a canonical schema (the table above) — 3 adapter functions, ~200 lines total.
4. **Publish.** Daily partition written via `pyarrow.parquet.write_to_dataset` with `zstd` compression, uploaded via `huggingface_hub.HfApi.upload_folder`.

No authentication required for any endpoint. Every URL hit is publicly documented or trivially discoverable in ATS client SDKs.

## What's covered / what isn't

**In scope:** the 3 "modern" developer-friendly ATSes. Heavy coverage of tech, startups, AI/ML, biotech, agencies, and other knowledge-work-heavy employers.

**Not in scope (yet):** Workday, Oracle Cloud HCM, iCIMS, SuccessFactors, Taleo, SmartRecruiters, Workable, Jobvite. These have either private APIs (Workday), high-volume-retail bias (SmartRecruiters), SMB noise (Workable), or HTML-only feeds (Jobvite). Collectively they host ~70% of enterprise postings but dilute signal for most downstream uses.

## Known limitations

- **~30% tenant fetch failure rate.** Slugs in the input list include defunct/renamed companies CC still references. These return 404 and are dropped.
- **No tombstoning.** A job present in `date=N` but absent in `date=N+1` is simply gone; there's no explicit `closed_at` flag. Compute by diffing.
- **No deduplication across ATSes.** A company listing the same role on both Greenhouse and Lever will appear twice. Rare in practice.
- **Salary fields are sparse and noisy.** Where structured comp is exposed, regex-parsed from the ATS's free-form `compensationTierSummary` string. Fall back to `description_html` for the full picture.

## License

MIT for the dataset packaging. The underlying job descriptions are copyright their respective employers and are redistributed under fair use for research and product purposes. If you are an employer and want a listing removed, open an issue on the GitHub repo.

## Citation

```
@misc{openapply2026,
  author = {edwarddgao},
  title  = {Open-Apply Jobs},
  year   = {2026},
  url    = {https://huggingface.co/datasets/edwarddgao/open-apply-jobs}
}
```
