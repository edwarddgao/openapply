"""Discover company slugs from Common Crawl and Simplify.jobs.

Two discovery sources:
1. Common Crawl — S3 direct access, extract slugs from CDX index shards
2. Simplify — resolve redirect URLs to extract ATS platform + slug

Usage:
    python -m openapply.discover                              # CC discovery, all ATS
    python -m openapply.discover --source cc --ats lever      # CC, single ATS
    python -m openapply.discover --source simplify            # Simplify redirect resolution
    python -m openapply.discover --source all                 # both sources
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx

from .simplify import _typesense_post

log = logging.getLogger("openapply.discover")

SLUGS_DIR = Path(__file__).parent.parent / "data" / "slugs"
PROVENANCE_PATH = SLUGS_DIR / "provenance.json"

# ATS domains and their URL patterns for slug extraction
ATS_DOMAINS = {
    "lever": {
        "surt_prefix": "co,lever,jobs)",
        "url_pattern": re.compile(r"jobs\.lever\.co/([a-zA-Z0-9._-]+)"),
        # Lever blocks CCBot since late 2025 — use older index
        "preferred_index": "CC-MAIN-2025-43",
    },
    "greenhouse": {
        "surt_prefix": "io,greenhouse,job-boards)",
        "url_pattern": re.compile(r"job-boards\.greenhouse\.io/([a-zA-Z0-9._-]+)"),
        "preferred_index": None,  # latest works
    },
    "ashby": {
        "surt_prefix": "com,ashbyhq,jobs)",
        "url_pattern": re.compile(r"jobs\.ashbyhq\.com/([a-zA-Z0-9%._-]+)"),
        "preferred_index": None,
    },
}

# Mapping from redirect URL hostname patterns → ATS name
ATS_HOST_PATTERNS = {
    "lever.co": "lever",
    "greenhouse.io": "greenhouse",
    "ashbyhq.com": "ashby",
}

CC_S3_BASE = "https://data.commoncrawl.org"


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def load_provenance(path: Path = PROVENANCE_PATH) -> dict[str, dict[str, list[str]]]:
    """Load provenance.json: {ats: {slug: [sources]}}."""
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_provenance(prov: dict[str, dict[str, list[str]]], path: Path = PROVENANCE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(prov, sort_keys=True, indent=1) + "\n")


def update_provenance(
    prov: dict[str, dict[str, list[str]]],
    ats: str,
    slugs: set[str],
    source: str,
) -> None:
    """Add source tag to slugs in provenance dict (mutates in place)."""
    ats_prov = prov.setdefault(ats, {})
    for slug in slugs:
        sources = ats_prov.setdefault(slug, [])
        if source not in sources:
            sources.append(source)


# ---------------------------------------------------------------------------
# Slug file I/O
# ---------------------------------------------------------------------------

def is_valid_slug(slug: str) -> bool:
    """Filter out junk slugs (URL params, fragments, numeric IDs, too short)."""
    if not slug or len(slug) <= 1:
        return False
    if any(c in slug for c in "?&=# "):
        return False
    if slug.isdigit():
        return False
    if slug.lower() in ("robots.txt", "sitemap.xml", "embed", "widget"):
        return False
    if slug.startswith(("api", "static", "assets", "favicon")):
        return False
    return True


def load_slugs(ats: str, slugs_dir: Path = SLUGS_DIR) -> set[str]:
    path = slugs_dir / f"{ats}.txt"
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def save_slugs(ats: str, slugs: set[str], slugs_dir: Path = SLUGS_DIR) -> None:
    slugs_dir.mkdir(parents=True, exist_ok=True)
    path = slugs_dir / f"{ats}.txt"
    path.write_text("\n".join(sorted(slugs)) + "\n")


def clean_slugs(slugs: set[str]) -> set[str]:
    """Remove junk slugs from a set."""
    return {s for s in slugs if is_valid_slug(s)}


def merge_slugs(
    ats: str,
    new_slugs: set[str],
    source: str,
    slugs_dir: Path = SLUGS_DIR,
    prov: dict | None = None,
) -> int:
    """Merge new slugs into slug file and update provenance. Returns count of new slugs."""
    new_slugs = clean_slugs(new_slugs)
    existing = load_slugs(ats, slugs_dir)
    merged = existing | new_slugs
    new_count = len(merged) - len(existing)
    save_slugs(ats, merged, slugs_dir)
    if prov is not None:
        update_provenance(prov, ats, new_slugs, source)
    return new_count


# ---------------------------------------------------------------------------
# Common Crawl discovery
# ---------------------------------------------------------------------------

def get_latest_index() -> str:
    """Get the latest CC index name from collinfo.json."""
    resp = httpx.get("https://index.commoncrawl.org/collinfo.json", timeout=15)
    if resp.status_code == 200:
        indices = resp.json()
        if indices:
            return indices[0]["id"]
    return "CC-MAIN-2025-43"


def find_shard_ranges(index_name: str, surt_prefix: str, client: httpx.Client) -> list[tuple[str, int, int]]:
    """Find which shard blocks in cluster.idx contain our SURT prefix.

    Returns list of (shard_filename, start_offset, length).
    """
    cluster_url = f"{CC_S3_BASE}/cc-index/collections/{index_name}/indexes/cluster.idx"

    log.info(f"  Downloading cluster.idx for {index_name}...")
    resp = client.get(cluster_url, timeout=120)
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    log.info(f"  cluster.idx: {len(lines)} entries")

    # cluster.idx is sorted; each line is the LAST key in a block.
    # Format: "SURT_KEY TIMESTAMP\tSHARD_FILE\tOFFSET\tLENGTH\tSEQNUM"
    matching_shards = []
    seen = set()
    for i, line in enumerate(lines):
        if line.startswith(surt_prefix) or (i > 0 and lines[i - 1] < surt_prefix <= line):
            parts = line.split("\t")
            if len(parts) >= 4:
                shard_file = parts[1]
                offset = int(parts[2])
                length = int(parts[3])
                key = (shard_file, offset)
                if key not in seen:
                    seen.add(key)
                    matching_shards.append((shard_file, offset, length))

    return matching_shards


def extract_slugs_from_shard(
    index_name: str, shard_file: str, offset: int, length: int,
    url_pattern: re.Pattern, client: httpx.Client,
) -> set[str]:
    """Download and decompress a shard block, extract slugs from CDX records."""
    url = f"{CC_S3_BASE}/cc-index/collections/{index_name}/indexes/{shard_file}"
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

    resp = client.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    try:
        data = gzip.decompress(resp.content)
    except gzip.BadGzipFile:
        log.warning(f"  Bad gzip block: {shard_file}@{offset}")
        return set()

    slugs = set()
    for line in data.decode("utf-8", errors="replace").split("\n"):
        match = url_pattern.search(line)
        if match:
            slug = unquote(match.group(1)).lower()
            if is_valid_slug(slug):
                slugs.add(slug)

    return slugs


def discover_cc(ats: str, output_dir: Path | None = None, prov: dict | None = None) -> set[str]:
    """Discover company slugs for an ATS from Common Crawl."""
    config = ATS_DOMAINS[ats]
    index_name = config["preferred_index"]
    if not index_name:
        index_name = get_latest_index()

    log.info(f"[{ats}] CC: using index {index_name}")

    client = httpx.Client(timeout=60)
    try:
        shards = find_shard_ranges(index_name, config["surt_prefix"], client)
        log.info(f"[{ats}] Found {len(shards)} shard blocks")

        all_slugs = set()
        for shard_file, offset, length in shards:
            if not shard_file:
                continue
            slugs = extract_slugs_from_shard(
                index_name, shard_file, offset, length,
                config["url_pattern"], client,
            )
            all_slugs.update(slugs)
            log.info(f"  Shard block: {len(slugs)} slugs ({len(all_slugs)} total)")

    finally:
        client.close()

    log.info(f"[{ats}] CC discovered {len(all_slugs)} unique slugs")

    if output_dir:
        new_count = merge_slugs(ats, all_slugs, "cc", output_dir, prov)
        total = len(load_slugs(ats, output_dir))
        log.info(f"[{ats}] Wrote {total} slugs ({new_count} new from CC)")

    return all_slugs


# ---------------------------------------------------------------------------
# Simplify redirect resolution
# ---------------------------------------------------------------------------

def _extract_ats_slug(url: str) -> tuple[str, str] | None:
    """Extract (ats, slug) from a resolved redirect URL. Returns None if unsupported ATS."""
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    path = parsed.path

    for domain_pattern, ats in ATS_HOST_PATTERNS.items():
        if domain_pattern in hostname:
            parts = path.strip("/").split("/")
            if parts and parts[0]:
                slug = unquote(parts[0]).lower()
                if is_valid_slug(slug):
                    return ats, slug
    return None


async def _resolve_batch(
    companies: list[tuple[str, str]],
    concurrency: int = 20,
) -> dict[str, set[str]]:
    """Resolve Simplify redirect URLs for a batch of (company_name, posting_id) pairs.

    Returns {ats: {slugs}}.
    """
    sem = asyncio.Semaphore(concurrency)
    results: dict[str, set[str]] = {}
    resolved = 0
    errors = 0

    async def resolve(client: httpx.AsyncClient, name: str, pid: str):
        nonlocal resolved, errors
        async with sem:
            try:
                r = await client.get(
                    f"https://simplify.jobs/jobs/click/{pid}",
                    follow_redirects=True,
                    timeout=10,
                )
                pair = _extract_ats_slug(str(r.url))
                if pair:
                    ats, slug = pair
                    results.setdefault(ats, set()).add(slug)
            except Exception:
                errors += 1
            resolved += 1
            if resolved % 500 == 0:
                log.info(f"  Resolved {resolved}/{len(companies)} ({errors} errors)")

    async with httpx.AsyncClient() as client:
        # Process in batches to avoid overwhelming the connection pool
        batch_size = 200
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            await asyncio.gather(*[resolve(client, n, p) for n, p in batch])

    log.info(f"  Resolved {resolved}/{len(companies)} ({errors} errors)")
    return results


def _fetch_simplify_companies(page: int, per_page: int = 250) -> list[tuple[str, str]]:
    """Fetch one page of (company_name, posting_id) from Simplify Typesense."""
    result = _typesense_post({"searches": [{
        "collection": "jobs",
        "q": "*",
        "query_by": "title",
        "group_by": "company_id",
        "per_page": per_page,
        "page": page,
    }]})

    companies = []
    for gh in result.get("grouped_hits", []):
        doc = gh["hits"][0]["document"]
        companies.append((doc["company_name"], doc["posting_id"]))
    return companies


def discover_simplify(output_dir: Path | None = None, prov: dict | None = None) -> dict[str, set[str]]:
    """Discover ATS slugs by resolving Simplify redirect URLs.

    Returns {ats: {slugs}}.
    """
    log.info("[simplify] Fetching companies from Typesense...")

    # Paginate to get all companies
    all_companies: list[tuple[str, str]] = []
    page = 1
    while True:
        batch = _fetch_simplify_companies(page)
        if not batch:
            break
        all_companies.extend(batch)
        log.info(f"  Page {page}: {len(batch)} companies ({len(all_companies)} total)")
        page += 1

    log.info(f"[simplify] Resolving {len(all_companies)} redirect URLs...")
    results = asyncio.run(_resolve_batch(all_companies))

    for ats, slugs in sorted(results.items()):
        log.info(f"[simplify] {ats}: {len(slugs)} slugs")
        if output_dir:
            new_count = merge_slugs(ats, slugs, "simplify", output_dir, prov)
            total = len(load_slugs(ats, output_dir))
            log.info(f"[simplify] {ats}: {total} total slugs ({new_count} new from Simplify)")

    return results


# ---------------------------------------------------------------------------
# Seed provenance (tag existing slugs)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def clean_slug_files(slugs_dir: Path, prov: dict[str, dict[str, list[str]]]) -> None:
    """Remove junk slugs from slug files and provenance."""
    for ats in ATS_DOMAINS:
        slugs = load_slugs(ats, slugs_dir)
        cleaned = clean_slugs(slugs)
        removed = slugs - cleaned
        if removed:
            save_slugs(ats, cleaned, slugs_dir)
            # Remove from provenance too
            ats_prov = prov.get(ats, {})
            for slug in removed:
                ats_prov.pop(slug, None)
            log.info(f"[{ats}] Cleaned {len(removed)} junk slugs ({len(cleaned)} remaining)")
        else:
            log.info(f"[{ats}] No junk slugs found ({len(cleaned)} total)")


def main():
    parser = argparse.ArgumentParser(description="Discover ATS slugs")
    parser.add_argument("--source", choices=["cc", "simplify", "all"], default="cc",
                        help="Discovery source (default: cc)")
    parser.add_argument("--ats", choices=list(ATS_DOMAINS.keys()),
                        help="Single ATS (CC mode only)")
    parser.add_argument("--clean", action="store_true",
                        help="Clean junk slugs from existing files")
    parser.add_argument("--output-dir", type=Path, default=SLUGS_DIR)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    prov = load_provenance()

    if args.clean:
        clean_slug_files(args.output_dir, prov)
        save_provenance(prov)
        log.info("Provenance saved.")
        return

    if args.source in ("cc", "all"):
        ats_list = [args.ats] if args.ats else list(ATS_DOMAINS.keys())
        for ats in ats_list:
            discover_cc(ats, args.output_dir, prov)

    if args.source in ("simplify", "all"):
        discover_simplify(args.output_dir, prov)

    save_provenance(prov)
    log.info("Provenance saved.")


if __name__ == "__main__":
    main()
