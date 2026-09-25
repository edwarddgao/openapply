#!/usr/bin/env python3
"""Discover live ATS tenant slugs from Simplify's Typesense job index.

The public Simplify client searches one Typesense document collection.  Job
documents omit the external application URL, but each document ID has a public
``/jobs/click/<id>`` redirect to the original posting.  This script samples the
most recently updated job for every company, extracts ATS tenant slugs from the
redirect targets, and keeps only tenants that still return live jobs.

Use ``--group-limit`` above 1 to inspect multiple recent jobs per company.  The
redirect cache makes broader or repeated passes incremental.
"""
from __future__ import annotations

import argparse
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


SIMPLIFY_JOBS_URL = "https://simplify.jobs/jobs"
SIMPLIFY_CLICK_URL = "https://simplify.jobs/jobs/click/{job_id}"
USER_AGENT = "Mozilla/5.0 (compatible; OpenApply slug discovery)"

SLUG_FILES = {
    "greenhouse": Path("slugs/cc_greenhouse_FINAL.txt"),
    "lever": Path("slugs/cc_lever_FINAL.txt"),
    "ashby": Path("slugs/cc_ashby_FINAL.txt"),
}

ATS_ENDPOINTS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
    "lever": "https://api.lever.co/v0/postings/{slug}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true",
}

SKIP_SLUGS = {"embed", "jobs", "api", "v0", "v1", "posting-api", "job-board"}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


NO_REDIRECT_OPENER = urllib.request.build_opener(NoRedirect)


def request_bytes(url: str, *, headers: dict[str, str] | None = None, timeout: float = 30) -> bytes:
    request_headers = {"User-Agent": USER_AGENT, "Referer": SIMPLIFY_JOBS_URL}
    request_headers.update(headers or {})
    request = urllib.request.Request(url, headers=request_headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def find_typesense_config() -> tuple[str, str]:
    html = request_bytes(SIMPLIFY_JOBS_URL).decode("utf-8", errors="replace")
    script_paths = sorted(set(re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', html)))

    def fetch_script(path: str) -> str:
        url = urllib.parse.urljoin(SIMPLIFY_JOBS_URL, path)
        try:
            return request_bytes(url).decode("utf-8", errors="replace")
        except Exception:
            return ""

    with ThreadPoolExecutor(max_workers=12) as executor:
        for script in executor.map(fetch_script, script_paths):
            host_match = re.search(r'nearestNode:\{host:"([^"]+)"', script)
            key_match = re.search(r'apiKey:"([^"]+)"', script)
            if host_match and key_match:
                return host_match.group(1), key_match.group(1)
    raise RuntimeError("Simplify Typesense configuration was not found in the current client bundles")


def typesense_search(host: str, api_key: str, params: dict[str, object]) -> dict:
    query = urllib.parse.urlencode(params)
    url = f"https://{host}/collections/jobs/documents/search?{query}"
    payload = request_bytes(url, headers={"X-TYPESENSE-API-KEY": api_key})
    return json.loads(payload)


def fetch_company_jobs(
    host: str,
    api_key: str,
    *,
    group_limit: int,
    max_companies: int,
) -> tuple[int, dict[str, list[dict]]]:
    companies: dict[str, list[dict]] = {}
    found = 0
    sort_orders = (
        ("asc", "shuffle_key:asc,posting_id:asc"),
        ("desc", "shuffle_key:desc,posting_id:desc"),
    )
    for direction, sort_by in sort_orders:
        page = 1
        offset = 0
        while True:
            data = typesense_search(
                host,
                api_key,
                {
                    "q": "*",
                    "query_by": "title",
                    # A stable shuffle key plus a posting-ID tie-breaker keeps
                    # each directional scan deterministic.  Scanning both
                    # directions covers Typesense's grouped-result window.
                    "sort_by": sort_by,
                    "group_by": "company_id",
                    "group_limit": group_limit,
                    "group_missing_values": "false",
                    "offset": offset,
                    "limit": 250,
                    "include_fields": "id,company_id,company_name,updated_date",
                },
            )
            found = max(found, int(data.get("found", 0)))
            groups = data.get("grouped_hits", [])
            if not groups:
                break
            for group in groups:
                hits = [hit.get("document", {}) for hit in group.get("hits", [])]
                hits = [hit for hit in hits if hit.get("id")]
                if not hits:
                    continue
                group_key = group.get("group_key") or []
                company_id = str(hits[0].get("company_id") or (group_key[0] if group_key else ""))
                if not company_id:
                    company_id = f"job:{hits[0]['id']}"
                companies[company_id] = hits
                if max_companies and len(companies) >= max_companies:
                    return found, companies
            print(
                f"typesense {direction} page {page}: {len(companies):,}/{found:,} companies",
                flush=True,
            )
            offset += len(groups)
            if len(companies) >= found:
                break
            page += 1
        if len(companies) >= found:
            break
    return found, companies


def load_redirect_cache(path: Path) -> dict[str, str | None]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def save_redirect_cache(path: Path, cache: dict[str, str | None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(cache, sort_keys=True))
    temporary.replace(path)


def resolve_click(job_id: str, timeout: float = 15) -> tuple[str, str | None]:
    request = urllib.request.Request(
        SIMPLIFY_CLICK_URL.format(job_id=job_id),
        headers={"User-Agent": USER_AGENT, "Referer": SIMPLIFY_JOBS_URL},
    )
    try:
        NO_REDIRECT_OPENER.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if 300 <= error.code < 400:
            return job_id, error.headers.get("Location")
    except Exception:
        pass
    return job_id, None


def extract_ats_slug(url: str | None) -> tuple[str, str] | None:
    if not url:
        return None
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    parts = [urllib.parse.unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    if not parts:
        return None

    ats = None
    if host in {"boards.greenhouse.io", "job-boards.greenhouse.io", "job-boards.eu.greenhouse.io"}:
        ats = "greenhouse"
    elif host in {"jobs.lever.co", "jobs.eu.lever.co"}:
        ats = "lever"
    elif host == "jobs.ashbyhq.com":
        ats = "ashby"
    if not ats:
        return None

    slug = parts[0]
    if slug.casefold() in SKIP_SLUGS:
        return None
    return ats, slug


def read_slugs(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def probe(ats: str, slug: str, timeout: float = 12) -> int:
    encoded_slug = urllib.parse.quote(slug, safe="")
    url = ATS_ENDPOINTS[ats].format(slug=encoded_slug)
    try:
        payload = request_bytes(url, timeout=timeout)
        data = json.loads(payload)
    except Exception:
        return 0
    if ats == "lever":
        return len(data) if isinstance(data, list) else 0
    return len(data.get("jobs", [])) if isinstance(data, dict) else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover ATS slugs from Simplify Typesense")
    parser.add_argument("--write", action="store_true", help="append verified slugs to FINAL files")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--group-limit", type=int, default=1, choices=range(1, 11))
    parser.add_argument("--max-companies", type=int, default=0, help="limit companies for a smoke test")
    parser.add_argument("--cache", type=Path, default=Path(".cache/simplify_redirects.json"))
    args = parser.parse_args()

    host, api_key = find_typesense_config()
    found, companies = fetch_company_jobs(
        host,
        api_key,
        group_limit=args.group_limit,
        max_companies=args.max_companies,
    )
    jobs = {hit["id"]: hit for hits in companies.values() for hit in hits}
    print(f"resolving {len(jobs):,} recent jobs from {len(companies):,}/{found:,} companies", flush=True)

    cache = load_redirect_cache(args.cache)
    unresolved_ids = sorted(set(jobs) - set(cache))
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for index, (job_id, destination) in enumerate(executor.map(resolve_click, unresolved_ids), start=1):
            cache[job_id] = destination
            if index % 500 == 0:
                save_redirect_cache(args.cache, cache)
                print(f"redirects: {index:,}/{len(unresolved_ids):,}", flush=True)
    save_redirect_cache(args.cache, cache)

    candidates: dict[str, set[str]] = {ats: set() for ats in SLUG_FILES}
    for job_id in jobs:
        extracted = extract_ats_slug(cache.get(job_id))
        if extracted:
            ats, slug = extracted
            candidates[ats].add(slug)

    known = {ats: read_slugs(path) for ats, path in SLUG_FILES.items()}
    missing = {
        ats: sorted(slug for slug in slugs if slug.casefold() not in {item.casefold() for item in known[ats]})
        for ats, slugs in candidates.items()
    }
    to_probe = [(ats, slug) for ats, slugs in missing.items() for slug in slugs]

    def check(item: tuple[str, str]) -> tuple[str, str, int]:
        ats, slug = item
        return ats, slug, probe(ats, slug)

    verified: dict[str, list[tuple[str, int]]] = {ats: [] for ats in SLUG_FILES}
    with ThreadPoolExecutor(max_workers=min(args.workers, 24)) as executor:
        for ats, slug, count in executor.map(check, to_probe):
            if count:
                verified[ats].append((slug, count))

    print(
        f"found {sum(len(value) for value in candidates.values()):,} ATS tenants; "
        f"{len(to_probe):,} absent locally; "
        f"{sum(len(value) for value in verified.values()):,} verified live",
        flush=True,
    )
    for ats in SLUG_FILES:
        for slug, count in sorted(verified[ats], key=lambda item: item[0].casefold()):
            print(f"{ats}\t{slug}\t{count}")

    if args.write:
        for ats, path in SLUG_FILES.items():
            additions = {slug for slug, _ in verified[ats]}
            if not additions:
                continue
            # Match the existing files' bytewise `sort -u` order.
            merged = sorted(known[ats] | additions)
            path.write_text("\n".join(merged) + "\n")
            print(f"appended {len(additions)} slugs to {path}", flush=True)


if __name__ == "__main__":
    main()
