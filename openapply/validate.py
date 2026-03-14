"""Validate discovery coverage — compare CC vs Simplify slug sets.

Uses raw discovery sets saved by discover.py (cc_latest.json, simplify_latest.json).
Run after discover.yml to understand how our index compares to Simplify's.

Usage:
    python -m openapply.validate                    # coverage report
    python -m openapply.validate --db jobs.db       # + job count comparison
"""

from __future__ import annotations

import argparse
import logging
import random
from pathlib import Path

from .db import get_connection, DB_PATH
from .discover import load_raw_sets, ATS_DOMAINS, SLUGS_DIR
from .simplify import count_company_jobs

log = logging.getLogger("openapply.validate")


def validate_coverage(slugs_dir: Path) -> dict:
    """Compare CC vs Simplify discovery sets and report coverage."""
    cc = load_raw_sets("cc", slugs_dir)
    simp = load_raw_sets("simplify", slugs_dir)

    if not cc and not simp:
        log.error("No raw discovery sets found. Run discover.py first.")
        return {}

    results = {}
    for ats in sorted(ATS_DOMAINS):
        cc_set = cc.get(ats, set())
        simp_set = simp.get(ats, set())

        cc_only = len(cc_set - simp_set)
        simp_only = len(simp_set - cc_set)
        both = len(cc_set & simp_set)
        union = len(cc_set | simp_set)

        results[ats] = {
            "cc": len(cc_set),
            "simplify": len(simp_set),
            "cc_only": cc_only,
            "simplify_only": simp_only,
            "both": both,
            "union": union,
        }

        log.info(
            f"[{ats}] CC={len(cc_set)} Simplify={len(simp_set)} | "
            f"CC-only={cc_only} Both={both} Simplify-only={simp_only} | "
            f"Union={union}"
        )

    # Totals
    total_cc_only = sum(r["cc_only"] for r in results.values())
    total_simp_only = sum(r["simplify_only"] for r in results.values())
    total_both = sum(r["both"] for r in results.values())
    log.info(
        f"[total] CC-only={total_cc_only} Both={total_both} "
        f"Simplify-only={total_simp_only}"
    )

    return results


def validate_job_counts(db_path: Path, sample_size: int = 50) -> dict:
    """Compare job counts for sampled companies between our DB and Simplify."""
    conn = get_connection(db_path)

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

    companies = [dict(r) for r in rows]
    sample = companies[:sample_size] if len(companies) <= sample_size else (
        companies[:sample_size // 2]
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
        "summary": {"sampled": len(comparisons), "valid": len(valid)},
    }


def main():
    parser = argparse.ArgumentParser(description="Validate discovery coverage")
    parser.add_argument("--slugs-dir", type=Path, default=SLUGS_DIR)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--sample", type=int, default=50)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    log.info("=== Discovery Coverage ===")
    validate_coverage(args.slugs_dir)

    if args.db.exists():
        log.info("\n=== Job Count Comparison ===")
        validate_job_counts(args.db, args.sample)


if __name__ == "__main__":
    main()
