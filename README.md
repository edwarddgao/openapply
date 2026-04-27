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
| `posted_at` | ISO 8601? | |
| `salary_min` / `salary_max` | float? | 25–35% populated (Ashby/Lever expose structured comp; Greenhouse embeds in HTML) |
| `salary_currency` / `salary_period` | str? | |

## Local run

```bash
pip install -r requirements.txt
python oa_adapter.py --workers 16 --out jobs.jsonl         # ~15 min
python scripts/jsonl_to_parquet.py jobs.jsonl data         # partition
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

## Embeddings

A companion dataset of `qwen3-embedding-8b` vectors over the postings, generated via [OpenRouter](https://openrouter.ai):

- **Dataset:** https://huggingface.co/datasets/edwarddgao/open-apply-jobs-embeddings
- **Model:** `qwen/qwen3-embedding-8b` (4096 dims, L2-normalizable). The embedding is Matryoshka — the first N dims (e.g. 512, 1024) are a valid lower-dim representation after re-normalization.
- **Content formula:** `title + "\n\n" + plain_text(description_html)` (bs4 `get_text`), capped at 30k plaintext chars / 32k-char batch-packing budget
- **Partitioning:** `date=YYYY-MM-DD/source={ats}/` — mirrors the base dataset so `id` joins 1:1
- **Columns:** `id`, `content_sha256`, `embedding` (list[float32]), `model`

The pipeline is incremental: each day's run reads yesterday's partition as a read-through cache, keyed on `(id, content_sha256)`, and only calls the API for new or changed postings.

```python
import duckdb
duckdb.sql("""
  SELECT j.id, j.title, e.embedding
  FROM read_parquet('hf://datasets/edwarddgao/open-apply-jobs/data/**/*.parquet', hive_partitioning=1) j
  JOIN read_parquet('hf://datasets/edwarddgao/open-apply-jobs-embeddings/data/**/*.parquet', hive_partitioning=1) e
    ON j.id = e.id
  WHERE j.date = '2026-04-17'
""")
```

Local run:

```bash
OPENROUTER_API_KEY=... python scripts/embed.py data embeddings
HF_TOKEN=... python scripts/publish_hf.py embeddings \
  --repo-id edwarddgao/open-apply-jobs-embeddings
```

## Enrichments

A second sidecar dataset adds per-row role clusters, industry clusters, and imputed salary derived from the embeddings:

- **Dataset:** https://huggingface.co/datasets/edwarddgao/open-apply-jobs-enrichments
- **Columns:** `id`, `content_sha256`, `role_l1_id`/`role_l1_name` (~25 coarse role buckets, e.g. Engineering / Sales / Clinical), `role_l2_id`/`role_l2_name` (~600 specific sub-roles, e.g. Senior Backend Engineer / PMHNP / BCBA), `industry_id`/`industry_name` (~80 industry verticals, e.g. openai / databricks / andurilindustries), `salary_lo_usd`/`salary_hi_usd`/`salary_mid_usd` (USD), `salary_src` (`column` | `regex` | `imputed`), `model`
- **How it's built:** role L1 from agglomerative clustering over department-string centroids (MRL-512 cosine, complete linkage); role L2 from MiniBatchKMeans on job embeddings within each L1 bucket; industry from the same agglomerative process over `source_slug` centroids; salary from Ridge regression on the full 4096-d embedding, trained on column + JD-body-regex observations (R²=0.75 on held-out, medAPE ~14% on rows whose JD doesn't mention salary).
- **Partitioning:** `date=YYYY-MM-DD/source={ats}/` — joins 1:1 on `id` with base and embeddings.

Models are trained once and checked into `models/` at repo root (~2 MB total). Daily enrichment inference uses the same SHA-keyed two-pass cache protocol as `embed.py`, so unchanged rows are copied forward from the previous day's partition.

```bash
# one-time model training (re-run if you want fresher clusters)
python scripts/train_enrichments.py --date YYYY-MM-DD --out-dir models/

# daily inference
python scripts/enrich.py data embeddings enrichments --date YYYY-MM-DD --bootstrap-from-hf
HF_TOKEN=... python scripts/publish_hf.py enrichments --repo-id edwarddgao/open-apply-jobs-enrichments
```

## License

- **Code:** MIT
- **Data:** Public job postings from public ATS APIs; the underlying listings belong to the companies that posted them. Redistributed as a convenience; use responsibly.
