"""Orchestrate scraping across all ATS platforms.

Usage:
    python -m openapply.scrape                    # scrape all
    python -m openapply.scrape --ats lever        # single ATS
    python -m openapply.scrape --slugs-dir data/slugs
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from pathlib import Path

from .db import (
    init_db, get_connection, upsert_company, upsert_job,
    mark_dead_company, purge_stale_jobs, batch_job_exists, set_meta, DB_PATH,
)
from .discover import load_slugs, SLUGS_DIR
from .normalize import content_hash
from .scrapers.lever import LeverScraper
from .scrapers.greenhouse import GreenhouseScraper
from .scrapers.ashby import AshbyScraper

log = logging.getLogger("openapply.scrape")

STALE_DAYS = 7
COMMIT_INTERVAL = 50


async def scrape_ats(scraper, slugs: list[str], db_path: Path) -> dict:
    """Scrape all slugs for one ATS. Returns stats dict."""
    stats = {"companies": 0, "dead": 0, "errors": 0, "jobs_new": 0, "jobs_updated": 0}
    ats = scraper.ats_name
    conn = get_connection(db_path)
    now = int(time.time())
    db_lock = asyncio.Lock()
    slugs_since_commit = 0
    processed = 0

    async def process_slug(slug: str):
        nonlocal slugs_since_commit, processed
        jobs = await scraper.probe_with_retry(slug)

        if jobs is None:
            company_id = f"{ats}:{slug}"
            async with db_lock:
                mark_dead_company(conn, company_id)
                slugs_since_commit += 1
                if slugs_since_commit >= COMMIT_INTERVAL:
                    conn.commit()
                    slugs_since_commit = 0
            stats["dead"] += 1
            processed += 1
            if processed % 100 == 0:
                log.info(
                    f"[{ats}] {processed}/{len(slugs)} slugs | "
                    f"{stats['jobs_new']} new, {stats['jobs_updated']} updated, "
                    f"{stats['dead']} dead, {stats['errors']} errors"
                )
            return

        stats["companies"] += 1

        # Consistent timestamp for all jobs in this scrape run
        for job in jobs:
            job["now"] = now

        # Determine company name from API response or DB
        company_name = next((j["company_name"] for j in jobs if j.get("company_name")), None)

        async with db_lock:
            if not company_name:
                row = conn.execute(
                    "SELECT name FROM companies WHERE company_id = ?",
                    (f"{ats}:{slug}",),
                ).fetchone()
                company_name = row["name"] if row else slug

            # Batch lookup of existing jobs (avoids N+1 queries)
            existing_map = batch_job_exists(conn, [j["job_id"] for j in jobs])

        # Classify jobs as new/update/touch (no DB access needed)
        for job in jobs:
            if not job.get("company_name"):
                job["company_name"] = company_name
            existing = existing_map.get(job["job_id"])

            if existing is None:
                job["_action"] = "new"
            else:
                lw_hash = content_hash(job["title"], slug, job["location_raw"] or "")
                if lw_hash != (existing.get("content_hash") or "")[:16]:
                    job["_action"] = "update"
                else:
                    job["_action"] = "touch"

        # Write all DB changes under lock
        async with db_lock:
            upsert_company(conn, {
                "company_id": f"{ats}:{slug}",
                "slug": slug,
                "ats": ats,
                "name": company_name,
                "last_probed_at": now,
            })

            for job in jobs:
                action = job.pop("_action")
                if action == "new":
                    upsert_job(conn, job)
                    stats["jobs_new"] += 1
                elif action == "update":
                    upsert_job(conn, job)
                    stats["jobs_updated"] += 1
                else:
                    conn.execute(
                        "UPDATE jobs SET last_seen_at = ? WHERE job_id = ?",
                        (now, job["job_id"]),
                    )

            slugs_since_commit += 1
            if slugs_since_commit >= COMMIT_INTERVAL:
                conn.commit()
                slugs_since_commit = 0

        processed += 1
        if processed % 100 == 0:
            log.info(
                f"[{ats}] {processed}/{len(slugs)} slugs | "
                f"{stats['jobs_new']} new, {stats['jobs_updated']} updated, "
                f"{stats['dead']} dead, {stats['errors']} errors"
            )

    # Process all slugs — scraper semaphore limits HTTP concurrency
    results = await asyncio.gather(
        *[process_slug(slug) for slug in slugs],
        return_exceptions=True,
    )
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            stats["errors"] += 1
            log.warning(f"[{ats}] {slugs[i]}: {result}")

    conn.commit()
    conn.close()

    log.info(
        f"[{ats}] Done: {processed}/{len(slugs)} slugs | "
        f"{stats['jobs_new']} new, {stats['jobs_updated']} updated, "
        f"{stats['dead']} dead, {stats['errors']} errors"
    )
    return stats


async def main():
    parser = argparse.ArgumentParser(description="Scrape ATS platforms for jobs")
    parser.add_argument("--ats", choices=["lever", "greenhouse", "ashby"],
                        help="Scrape only this ATS")
    parser.add_argument("--slugs-dir", type=Path, default=SLUGS_DIR)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    init_db(args.db)
    now = int(time.time())

    scrapers = {
        "lever": LeverScraper,
        "greenhouse": GreenhouseScraper,
        "ashby": AshbyScraper,
    }

    ats_list = [args.ats] if args.ats else list(scrapers.keys())
    all_stats = {}

    for ats_name in ats_list:
        slugs = sorted(load_slugs(ats_name, args.slugs_dir))
        if not slugs:
            log.warning(f"No slugs for {ats_name}, skipping")
            continue

        log.info(f"[{ats_name}] Scraping {len(slugs)} companies...")
        scraper = scrapers[ats_name]()

        try:
            stats = await scrape_ats(scraper, slugs, args.db)
            all_stats[ats_name] = stats
        finally:
            await scraper.close()

    # Purge stale jobs
    cutoff = now - (STALE_DAYS * 86400)
    conn = get_connection(args.db)
    purged = purge_stale_jobs(conn, cutoff)
    set_meta(conn, "last_scrape_at", str(now))
    conn.commit()
    conn.close()

    if purged:
        log.info(f"Purged {purged} stale jobs (not seen in {STALE_DAYS} days)")

    # Summary
    total_new = sum(s.get("jobs_new", 0) for s in all_stats.values())
    total_updated = sum(s.get("jobs_updated", 0) for s in all_stats.values())
    total_companies = sum(s.get("companies", 0) for s in all_stats.values())
    log.info(f"Total: {total_companies} companies, {total_new} new jobs, {total_updated} updated, {purged} purged")


if __name__ == "__main__":
    asyncio.run(main())
