#!/usr/bin/env python3
"""Harvest ATS tenant slugs from Common Crawl CDX.

Queries recent CC snapshots for URLs matching public ATS job-board
hostnames, extracts the tenant-slug path segment, dedups, and merges
into slugs/cc_{ats}_FINAL.txt.

Merge semantics: existing slugs are preserved (so slugs added via the
one-time Simplify bootstrap survive a re-run). Intended for monthly
cadence — CC index lags by ~1 week and tenant churn is slow.
"""
import argparse, json, re, time, urllib.request
from pathlib import Path
from collections import defaultdict

COLLINFO_URL = 'https://index.commoncrawl.org/collinfo.json'

ATS = {
    'greenhouse': ('boards.greenhouse.io/*',
                   re.compile(r'boards\.greenhouse\.io/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'embed', 'jobs', 'api', 'v1', 'assets', 'static'}),
    'lever':      ('jobs.lever.co/*',
                   re.compile(r'jobs\.lever\.co/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'js', 'api', 'embed', 'assets', 'robots.txt', 'sitemap.xml', 'favicon.ico'}),
    'ashby':      ('jobs.ashbyhq.com/*',
                   re.compile(r'jobs\.ashbyhq\.com/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'api', 'embed', 'assets', 'static', 'common'}),
}


def cdx_get(url, timeout=240):
    req = urllib.request.Request(url, headers={'User-Agent': 'openapply-harvest/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', errors='replace')


def discover_crawls(n):
    """Return the N newest CC-MAIN crawl IDs, sorted newest-first.

    IDs like CC-MAIN-YYYY-WW are lexicographically sortable, so alphabetic
    descending sort == chronological descending.
    """
    for _ in range(3):
        try:
            data = json.loads(cdx_get(COLLINFO_URL, 30))
            ids = sorted((c['id'] for c in data if c.get('id', '').startswith('CC-MAIN-')), reverse=True)
            return ids[:n]
        except Exception:
            time.sleep(5)
    raise RuntimeError(f'could not fetch {COLLINFO_URL}')


def num_pages(crawl, pattern):
    u = f'https://index.commoncrawl.org/{crawl}-index?url={pattern}&showNumPages=true'
    for _ in range(4):
        try:
            txt = cdx_get(u, 30)
            if txt.startswith('{'):
                return json.loads(txt)['pages']
        except Exception:
            pass
        time.sleep(3)
    return 0


def fetch_page(crawl, pattern, page):
    u = f'https://index.commoncrawl.org/{crawl}-index?url={pattern}&output=json&page={page}'
    for i in range(6):
        try:
            txt = cdx_get(u, 240)
            if txt and txt.startswith('{'):
                return txt
        except Exception:
            pass
        time.sleep(5 * (i + 1))
    return ''


def extract_slugs(jsonl, pat, skip):
    out = set()
    for ln in jsonl.splitlines():
        if not ln.startswith('{'):
            continue
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        m = pat.search(rec.get('url', ''))
        if not m:
            continue
        s = m.group(1).lower().rstrip('.-_')
        if s and s not in skip and len(s) >= 2:
            out.add(s)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--slug-dir', default='slugs')
    ap.add_argument('--n-crawls', type=int, default=5, help='number of most-recent CC snapshots to query')
    args = ap.parse_args()

    out_dir = Path(args.slug_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    crawls = discover_crawls(args.n_crawls)
    print(f'crawls: {", ".join(crawls)}', flush=True)

    discovered = defaultdict(set)
    for crawl in crawls:
        print(f'=== {crawl} ===', flush=True)
        for ats, (pat_url, regex, skip) in ATS.items():
            pages = num_pages(crawl, pat_url)
            if pages == 0:
                print(f'  {ats:<12} pages=?? skipped', flush=True)
                continue
            chunks = [fetch_page(crawl, pat_url, p) for p in range(pages)]
            slugs = extract_slugs('\n'.join(c for c in chunks if c), regex, skip)
            discovered[ats] |= slugs
            print(f'  {ats:<12} pages={pages} slugs={len(slugs):,}', flush=True)

    print('\n=== merge ===', flush=True)
    for ats, new_slugs in discovered.items():
        path = out_dir / f'cc_{ats}_FINAL.txt'
        existing = set(ln.strip() for ln in path.read_text().splitlines() if ln.strip()) if path.exists() else set()
        merged = existing | new_slugs
        path.write_text('\n'.join(sorted(merged)) + '\n')
        added = len(merged - existing)
        print(f'  {ats:<12} existing={len(existing):>5} discovered={len(new_slugs):>5} added={added:>4} total={len(merged):>5}', flush=True)


if __name__ == '__main__':
    main()
