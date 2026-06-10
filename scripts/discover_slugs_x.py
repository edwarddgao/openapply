#!/usr/bin/env python3
"""Discover new ATS slugs from X (Twitter) hiring posts.

Feed it the text of X search results / posts on stdin. It does two things:

  1. DIRECT: pulls slugs straight out of any greenhouse/lever/ashby URLs in the
     text (the slug is in the URL path -- no guessing).
  2. PROBE:  takes company names from "Company | role | link" lines (the format
     the x-search skill is asked to emit) plus any --names, normalizes each to
     candidate slugs, and hits the live ATS JSON endpoints. Keeps only candidates
     that return live jobs.

Every candidate is deduped against the existing slug files. Only slugs that are
(a) not already known and (b) return live postings are reported. With --write the
verified-new slugs are appended to slugs/cc_{ats}_FINAL.txt (case-sensitive
`sort -u`, matching the existing file format).

Usage:
  python3 ~/.claude/skills/x-search/x_search.py "<hiring query>" \
    | python scripts/discover_slugs_x.py
  ... | python scripts/discover_slugs_x.py --write          # also append
  python scripts/discover_slugs_x.py --names "Acme AI" "Foo" # probe names only
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SLUG_FILES = {
    "greenhouse": Path("slugs/cc_greenhouse_FINAL.txt"),
    "lever": Path("slugs/cc_lever_FINAL.txt"),
    "ashby": Path("slugs/cc_ashby_FINAL.txt"),
}

# slug is the path segment right after these hosts
URL_PATTERNS = {
    "greenhouse": re.compile(r"(?:job-)?boards(?:-api)?\.greenhouse\.io/(?:v1/boards/)?([A-Za-z0-9_-]+)", re.I),
    "lever": re.compile(r"(?:jobs|api)\.lever\.co/(?:v0/postings/)?([A-Za-z0-9_-]+)", re.I),
    "ashby": re.compile(r"(?:jobs|api)\.ashbyhq\.com/(?:posting-api/job-board/)?([A-Za-z0-9_-]+)", re.I),
}

ENDPOINTS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{s}/jobs",
    "lever": "https://api.lever.co/v0/postings/{s}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{s}?includeCompensation=true",
}

# generic path tokens that are not tenant slugs
URL_STOPWORDS = {"v1", "v0", "boards", "postings", "job-board", "posting-api", "jobs", "j"}

SUFFIXES = ("technologies", "technology", "labs", "lab", "inc", "llc", "ai", "io",
            "hq", "app", "co", "corp", "software", "systems", "health", "bio")
PREFIXES = ("the", "get", "join", "try", "use", "with", "go")


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def name_variants(name: str) -> list[str]:
    base = norm(name)
    out = {base}
    for suf in SUFFIXES:
        if base.endswith(suf) and len(base) > len(suf) + 2:
            out.add(base[: -len(suf)])
    for pre in PREFIXES:
        if base.startswith(pre) and len(base) > len(pre) + 2:
            out.add(base[len(pre):])
    # also a hyphenated form for multi-word names: "Oasis Health" -> "oasis-health"
    words = re.findall(r"[a-z0-9]+", name.lower())
    if len(words) > 1:
        out.add("-".join(words))
    return [v for v in out if len(v) >= 2]


def read_slugs(fp: Path) -> set[str]:
    """Line-based read; slug files may contain space-containing entries."""
    if not fp.exists():
        return set()
    return {ln.strip() for ln in fp.read_text().splitlines() if ln.strip()}


def load_known() -> dict[str, set[str]]:
    return {ats: read_slugs(fp) for ats, fp in SLUG_FILES.items()}


def probe(ats: str, slug: str, timeout: float = 8.0) -> int:
    """Return live job count for slug on ats (0 if none / error)."""
    url = ENDPOINTS[ats].format(s=slug)
    req = urllib.request.Request(url, headers={"User-Agent": "oa-slug-discovery"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status != 200:
                return 0
            data = json.loads(r.read())
    except Exception:
        return 0
    if ats == "lever":
        return len(data) if isinstance(data, list) else 0
    return len(data.get("jobs", [])) if isinstance(data, dict) else 0


def extract_company_names(text: str) -> list[str]:
    """Pull company names from 'Company | role | link' or '**Company** |' lines."""
    names = []
    for line in text.splitlines():
        line = line.strip().lstrip("-*•— ").strip()
        if "|" not in line:
            continue
        head = line.split("|", 1)[0]
        name = re.sub(r"\(.*?\)", "", head).replace("*", "").strip()
        if 2 <= len(name) <= 60 and not name.lower().startswith(
            ("here", "format", "note", "these", "links", "company")
        ):
            names.append(name)
    return names


def main() -> None:
    ap = argparse.ArgumentParser(description="Discover new ATS slugs from X hiring posts")
    ap.add_argument("--names", nargs="*", default=[], help="extra company names to probe")
    ap.add_argument("--write", action="store_true", help="append verified-new slugs to slug files")
    ap.add_argument("--no-probe-names", action="store_true", help="only extract slugs from URLs, skip name probing")
    args = ap.parse_args()

    text = "" if sys.stdin.isatty() else sys.stdin.read()
    known = load_known()
    known_all = {s for sset in known.values() for s in sset}

    # Path 1: direct slugs from URLs -> (ats, slug)
    direct: set[tuple[str, str]] = set()
    for ats, pat in URL_PATTERNS.items():
        for m in pat.finditer(text):
            slug = m.group(1)
            if slug.lower() not in URL_STOPWORDS:
                direct.add((ats, slug))

    # Path 2: candidate (ats, slug) from company names (probe every ATS)
    names = extract_company_names(text) + args.names
    candidates: set[tuple[str, str]] = set()
    if not args.no_probe_names:
        for name in names:
            for v in name_variants(name):
                for ats in ENDPOINTS:
                    candidates.add((ats, v))

    # dedupe vs known; combine. Direct slugs are also verified by probe.
    to_check = {(ats, s) for ats, s in (direct | candidates) if s not in known_all}

    def check(item):
        ats, slug = item
        return ats, slug, probe(ats, slug)

    verified: dict[str, list[tuple[str, int]]] = {ats: [] for ats in ENDPOINTS}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for ats, slug, n in ex.map(check, sorted(to_check)):
            if n > 0:
                verified[ats].append((slug, n))

    total = sum(len(v) for v in verified.values())
    print(f"# parsed {len(names)} names, {len(direct)} URL slugs, {len(to_check)} new candidates probed")
    print(f"# {total} verified-new slugs (return live jobs, not already known)\n")
    for ats in ENDPOINTS:
        for slug, n in sorted(verified[ats]):
            print(f"{ats}\t{slug}\t{n} jobs")

    if args.write and total:
        for ats, fp in SLUG_FILES.items():
            new = [s for s, _ in verified[ats]]
            if not new:
                continue
            merged = sorted(read_slugs(fp) | set(new))  # line-based, matches harvest_cc.py
            fp.write_text("\n".join(merged) + "\n")
            print(f"\n# appended {len(new)} slug(s) to {fp}", file=sys.stderr)


if __name__ == "__main__":
    main()
