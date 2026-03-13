"""Validate our DB against Simplify.jobs.

Two levels of validation:
1. Company coverage — compare simplify_ats_map.json against our slug files
2. Job coverage (sampled) — compare job counts for shared companies

Usage:
    python -m openapply.validate --slugs-dir data/slugs
    python -m openapply.validate --db jobs.db --sample 50
"""

from __future__ import annotations

import argparse
import json
import logging
import random
from pathlib import Path

from .db import get_connection, DB_PATH
from .discover import load_slugs, SLUGS_DIR
from .simplify import count_company_jobs

log = logging.getLogger("openapply.validate")


def validate_company_coverage(ats_map_path: Path, slugs_dir: Path) -> dict:
    """Compare simplify_ats_map.json against our slug files.

    ats_map_path: JSON file mapping simplify company_id → {ats, slug, company_name}
    Returns stats per ATS.
    """
    with open(ats_map_path) as f:
        ats_map = json.load(f)

    # Group Simplify companies by ATS
    simplify_by_ats: dict[str, list[dict]] = {}
    for entry in ats_map.values() if isinstance(ats_map, dict) else ats_map:
        ats = entry.get("ats")
        if ats:
            simplify_by_ats.setdefault(ats, []).append(entry)

    results = {}
    for ats, companies in sorted(simplify_by_ats.items()):
        our_slugs = load_slugs(ats, slugs_dir)
        simplify_slugs = {c["slug"] for c in companies if c.get("slug")}

        in_both = our_slugs & simplify_slugs
        only_ours = our_slugs - simplify_slugs
        only_simplify = simplify_slugs - our_slugs

        results[ats] = {
            "simplify": len(simplify_slugs),
            "ours": len(our_slugs),
            "overlap": len(in_both),
            "only_ours": len(only_ours),
            "only_simplify": len(only_simplify),
            "missing_slugs": sorted(only_simplify)[:20],  # sample
        }

        coverage = len(in_both) / len(simplify_slugs) * 100 if simplify_slugs else 0
        log.info(
            f"[{ats}] Simplify: {len(simplify_slugs)}, Ours: {len(our_slugs)}, "
            f"Overlap: {len(in_both)} ({coverage:.1f}%), "
            f"Missing from us: {len(only_simplify)}, Extra: {len(only_ours)}"
        )

    return results


def validate_job_counts(db_path: Path, sample_size: int = 50) -> dict:
    """Compare job counts for sampled companies between our DB and Simplify.

    Returns list of {company, ours, simplify, diff}.
    """
    conn = get_connection(db_path)

    # Get companies with job counts from our DB
    rows = conn.execute("""
        SELECT company_name, COUNT(*) as job_count
        FROM jobs
        WHERE company_name IS NOT NULL
        GROUP BY company_name
        ORDER BY job_count DESC
    """).fetchall()
    conn.close()

    if not rows:
        log.warning("No jobs in DB")
        return {"comparisons": [], "summary": {}}

    # Sample companies (bias toward larger ones for better signal)
    companies = [dict(r) for r in rows]
    sample = companies[:sample_size] if len(companies) <= sample_size else (
        companies[:sample_size // 2]  # top half by size
        + random.sample(companies[sample_size // 2:], min(sample_size // 2, len(companies) - sample_size // 2))
    )

    comparisons = []
    for comp in sample:
        name = comp["company_name"]
        our_count = comp["job_count"]
        try:
            simplify_count = count_company_jobs(name)
        except Exception as e:
            log.debug(f"Failed to query Simplify for {name}: {e}")
            simplify_count = -1

        diff = our_count - simplify_count if simplify_count >= 0 else None
        comparisons.append({
            "company": name,
            "ours": our_count,
            "simplify": simplify_count,
            "diff": diff,
        })

        if diff is not None and abs(diff) > max(5, our_count * 0.3):
            log.warning(f"  {name}: ours={our_count}, simplify={simplify_count}, diff={diff:+d}")

    # Summary
    valid = [c for c in comparisons if c["diff"] is not None]
    if valid:
        avg_diff = sum(c["diff"] for c in valid) / len(valid)
        more = sum(1 for c in valid if c["diff"] > 0)
        less = sum(1 for c in valid if c["diff"] < 0)
        equal = sum(1 for c in valid if c["diff"] == 0)
        log.info(f"Job count comparison ({len(valid)} companies): avg diff={avg_diff:+.1f}, "
                 f"we have more={more}, less={less}, equal={equal}")

    return {
        "comparisons": comparisons,
        "summary": {
            "sampled": len(comparisons),
            "valid": len(valid),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Validate against Simplify.jobs")
    parser.add_argument("--ats-map", type=Path, help="Path to simplify_ats_map.json")
    parser.add_argument("--slugs-dir", type=Path, default=SLUGS_DIR)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--sample", type=int, default=50, help="Companies to sample for job count comparison")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.ats_map:
        log.info("=== Company Coverage ===")
        validate_company_coverage(args.ats_map, args.slugs_dir)

    if args.db.exists():
        log.info("\n=== Job Count Comparison ===")
        validate_job_counts(args.db, args.sample)
    else:
        log.warning(f"DB not found: {args.db}")


if __name__ == "__main__":
    main()
