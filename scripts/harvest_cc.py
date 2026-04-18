#!/usr/bin/env python3
"""Harvest ATS tenant slugs from Common Crawl's CDX data on S3.

Uses the data-path directly (cluster.idx + cdx-*.gz on data.commoncrawl.org)
rather than the frequently-flaky index.commoncrawl.org HTTP API. The nginx
frontend occasionally goes down for days at a time; the underlying files on
CloudFront stay up throughout, so this is more resilient.

Algorithm:
  1. discover_crawls() via collinfo.json (static, reliable)
  2. Per crawl: download cluster.idx (sparse SURT index, ~110MB, cached locally)
  3. Per ATS host SURT prefix: locate matching byte-ranges in cluster.idx
  4. HTTP Range-fetch those ranges from cdx-NNNNN.gz
  5. Gunzip, parse CDX lines, extract slugs, merge into slugs/

Merge semantics: existing slug files are preserved (so Simplify-bootstrapped
entries survive a re-run).
"""
import argparse, gzip, json, re, time, urllib.request
from pathlib import Path
from collections import defaultdict

CDN = 'https://data.commoncrawl.org'
COLLINFO_URL = 'https://index.commoncrawl.org/collinfo.json'

# SURT prefix + slug regex + skip set, per ATS.
ATS = {
    'greenhouse': ('io,greenhouse,boards)',
                   re.compile(r'boards\.greenhouse\.io/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'embed', 'jobs', 'api', 'v1', 'assets', 'static'}),
    'lever':      ('co,lever,jobs)',
                   re.compile(r'jobs\.lever\.co/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'js', 'api', 'embed', 'assets', 'robots.txt', 'sitemap.xml', 'favicon.ico'}),
    'ashby':      ('com,ashbyhq,jobs)',
                   re.compile(r'jobs\.ashbyhq\.com/([a-zA-Z0-9][\w.-]{1,48})', re.I),
                   {'api', 'embed', 'assets', 'static', 'common'}),
}


def http_get(url, range_=None, timeout=600):
    headers = {'User-Agent': 'openapply-harvest/1.0'}
    if range_:
        headers['Range'] = f'bytes={range_[0]}-{range_[1]}'
    req = urllib.request.Request(url, headers=headers)
    last = None
    for i in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            time.sleep(5 * (i + 1))
    raise RuntimeError(f'failed: {url} ({last})')


def discover_crawls(n):
    data = json.loads(http_get(COLLINFO_URL, timeout=60).decode())
    ids = sorted((c['id'] for c in data if c.get('id', '').startswith('CC-MAIN-')), reverse=True)
    return ids[:n]


def fetch_cluster_idx(crawl, cache_dir):
    path = cache_dir / f'{crawl}.cluster.idx'
    if path.exists() and path.stat().st_size > 1_000_000:
        return path.read_bytes()
    print(f'  downloading cluster.idx ({crawl})...', flush=True)
    data = http_get(f'{CDN}/cc-index/collections/{crawl}/indexes/cluster.idx', timeout=600)
    path.write_bytes(data)
    return data


def find_ranges(idx_bytes, surt_prefix):
    """Return (cdx_file, offset, length) byte-ranges whose CDX entries may
    have SURT starting with surt_prefix.

    cluster.idx is SURT-sorted; each line marks the start of a chunk. We emit
    the last chunk whose surt < prefix (covers the tail preceding the match)
    plus every chunk whose surt falls within [prefix, prefix+\\xff].
    """
    ranges = []
    prev = None
    end_key = surt_prefix + '\xff'
    seen_match = False
    for line in idx_bytes.decode('utf-8', errors='replace').splitlines():
        parts = line.split('\t')
        if len(parts) < 5:
            continue
        surt = parts[0].split(' ', 1)[0]
        if surt < surt_prefix:
            prev = parts
            continue
        if surt > end_key:
            break
        if not seen_match:
            if prev is not None:
                ranges.append((prev[1], int(prev[2]), int(prev[3])))
            seen_match = True
        ranges.append((parts[1], int(parts[2]), int(parts[3])))
    if not seen_match and prev is not None:
        ranges.append((prev[1], int(prev[2]), int(prev[3])))
    return ranges


def extract_from_cdx_range(crawl, cdx_file, offset, length, regex, skip):
    url = f'{CDN}/cc-index/collections/{crawl}/indexes/{cdx_file}'
    try:
        gz = http_get(url, range_=(offset, offset + length - 1), timeout=120)
        raw = gzip.decompress(gz).decode('utf-8', errors='replace')
    except Exception as e:
        print(f'    warn: {cdx_file}@{offset}: {e}', flush=True)
        return set()
    slugs = set()
    for line in raw.splitlines():
        parts = line.split(' ', 2)
        if len(parts) < 3:
            continue
        try:
            meta = json.loads(parts[2])
        except Exception:
            continue
        m = regex.search(meta.get('url', ''))
        if not m:
            continue
        s = m.group(1).lower().rstrip('.-_')
        if s and s not in skip and len(s) >= 2:
            slugs.add(s)
    return slugs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--slug-dir', default='slugs')
    ap.add_argument('--n-crawls', type=int, default=5, help='number of most-recent CC snapshots to query')
    ap.add_argument('--cache-dir', default='.cache/cc', help='where to cache cluster.idx files')
    args = ap.parse_args()

    out_dir = Path(args.slug_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    crawls = discover_crawls(args.n_crawls)
    print(f'crawls: {", ".join(crawls)}', flush=True)

    discovered = defaultdict(set)
    for crawl in crawls:
        print(f'=== {crawl} ===', flush=True)
        idx = fetch_cluster_idx(crawl, cache_dir)
        for ats, (prefix, regex, skip) in ATS.items():
            ranges = find_ranges(idx, prefix)
            if not ranges:
                print(f'  {ats:<12} no matching cluster.idx ranges', flush=True)
                continue
            slugs = set()
            for cdx, off, ln in ranges:
                slugs |= extract_from_cdx_range(crawl, cdx, off, ln, regex, skip)
            discovered[ats] |= slugs
            print(f'  {ats:<12} ranges={len(ranges):>3} slugs={len(slugs):,}', flush=True)

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
