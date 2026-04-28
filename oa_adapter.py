#!/usr/bin/env python3
"""Open-Apply: 3 ATS adapters → canonical JobPosting JSONL.

Usage:
  python3 oa_adapter.py --slug-dir slugs --out jobs.jsonl --limit 10
"""
import argparse, json, sys, time, urllib.request, urllib.error, urllib.parse, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any

UA = 'Mozilla/5.0 (open-apply/0.1)'
TIMEOUT = 20

@dataclass
class JobPosting:
    id: str                         # {source}:{slug}:{native_id}
    source: str                     # greenhouse|lever|ashby|workable|smartrecruiters
    source_slug: str                # tenant slug on the ATS
    title: str
    apply_url: str
    description_html: Optional[str] = None
    employment_type: Optional[str] = None
    department: Optional[str] = None
    locations: List[str] = field(default_factory=list)
    remote: Optional[bool] = None
    posted_at: Optional[str] = None    # original publish/create timestamp, ISO 8601
    updated_at: Optional[str] = None   # last updated timestamp, ISO 8601 when exposed
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    salary_period: Optional[str] = None  # HOUR|DAY|WEEK|MONTH|YEAR

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}

def http_get(url: str, timeout: int = TIMEOUT, retries: int = 2) -> bytes:
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code not in RETRYABLE_STATUS: raise
            last = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
        time.sleep(1 + attempt)
    raise last

def http_json(url: str, timeout: int = TIMEOUT) -> Any:
    return json.loads(http_get(url, timeout))

# ---------- Greenhouse ----------
def fetch_greenhouse(slug: str) -> List[JobPosting]:
    url = f'https://boards-api.greenhouse.io/v1/boards/{urllib.parse.quote(slug)}/jobs?content=true'
    d = http_json(url)
    out = []
    for j in d.get('jobs', []):
        loc_name = (j.get('location') or {}).get('name') or ''
        locs = [loc_name] if loc_name else []
        remote = ('remote' in loc_name.lower()) if loc_name else None
        out.append(JobPosting(
            id=f"greenhouse:{slug}:{j.get('id')}",
            source='greenhouse', source_slug=slug,
            title=j.get('title') or '',
            apply_url=j.get('absolute_url') or '',
            description_html=j.get('content'),
            department=next((d.get('name') for d in (j.get('departments') or []) if isinstance(d, dict)), None),
            locations=locs, remote=remote,
            posted_at=j.get('first_published'),
            updated_at=j.get('updated_at'),
        ))
    return out

# ---------- Lever ----------
def fetch_lever(slug: str) -> List[JobPosting]:
    url = f'https://api.lever.co/v0/postings/{urllib.parse.quote(slug)}?mode=json&limit=500'
    d = http_json(url)
    out = []
    if not isinstance(d, list): return out
    for j in d:
        cat = j.get('categories') or {}
        sal = j.get('salaryRange') or {}
        out.append(JobPosting(
            id=f"lever:{slug}:{j.get('id')}",
            source='lever', source_slug=slug,
            title=j.get('text') or '',
            apply_url=j.get('hostedUrl') or j.get('applyUrl') or '',
            description_html=j.get('descriptionPlain') or j.get('description'),
            employment_type=cat.get('commitment'),
            department=cat.get('team') or cat.get('department'),
            locations=[cat.get('location')] if cat.get('location') else [],
            remote=('remote' in (cat.get('location','').lower())) if cat.get('location') else None,
            posted_at=_epoch_to_iso(j.get('createdAt')),
            updated_at=_epoch_to_iso(j.get('updatedAt')),
            salary_min=sal.get('min'),
            salary_max=sal.get('max'),
            salary_currency=sal.get('currency'),
            salary_period=(sal.get('interval') or '').upper() or None,
        ))
    return out

def _epoch_to_iso(ms):
    if not ms: return None
    try:
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ms/1000.0))
    except (TypeError, ValueError, OSError):
        return None

# ---------- Ashby ----------
def fetch_ashby(slug: str) -> List[JobPosting]:
    url = f'https://api.ashbyhq.com/posting-api/job-board/{urllib.parse.quote(slug)}?includeCompensation=true'
    d = http_json(url)
    out = []
    for j in d.get('jobs', []):
        sec_locs = [l.get('location') if isinstance(l, dict) else l for l in (j.get('secondaryLocations') or [])]
        comp = (j.get('compensation') or {}).get('compensationTierSummary') or ''
        sal_min = sal_max = sal_curr = None
        # Parse "$180K - $220K USD" style strings — best-effort
        m = re.search(r'([$£€])\s*(\d[\d,.kKmM]*)\s*(?:-|–|to)\s*([$£€]?)\s*(\d[\d,.kKmM]*)\s*([A-Z]{3})?', comp)
        if m:
            sal_min = _parse_num(m.group(2))
            sal_max = _parse_num(m.group(4))
            sal_curr = m.group(5) or {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1))
        out.append(JobPosting(
            id=f"ashby:{slug}:{j.get('id')}",
            source='ashby', source_slug=slug,
            title=j.get('title') or '',
            apply_url=j.get('jobUrl') or f'https://jobs.ashbyhq.com/{slug}/{j.get("id")}',
            description_html=j.get('descriptionHtml') or j.get('description'),
            employment_type=j.get('employmentType'),
            department=j.get('department') or j.get('team'),
            locations=[j.get('location')] + sec_locs if j.get('location') else sec_locs,
            remote=j.get('isRemote'),
            posted_at=j.get('publishedAt'),
            updated_at=j.get('updatedAt'),
            salary_min=sal_min, salary_max=sal_max,
            salary_currency=sal_curr,
        ))
    return out

def _parse_num(s: str) -> Optional[float]:
    s = s.replace(',', '').strip().lower()
    mul = 1
    if s.endswith('k'): mul, s = 1_000, s[:-1]
    elif s.endswith('m'): mul, s = 1_000_000, s[:-1]
    try: return float(s) * mul
    except ValueError: return None

ADAPTERS = {
    'greenhouse': fetch_greenhouse,
    'lever':      fetch_lever,
    'ashby':      fetch_ashby,
}

def run_adapter(ats: str, slug: str) -> tuple:
    try:
        jobs = ADAPTERS[ats](slug)
        return (ats, slug, jobs, None)
    except Exception as e:
        return (ats, slug, [], f'{type(e).__name__}: {e}')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--slug-dir', default='slugs', help='dir with cc_{ats}_FINAL.txt files')
    ap.add_argument('--out', default='oa_jobs.jsonl')
    ap.add_argument('--limit', type=int, default=0, help='max slugs per ATS (0=all)')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--ats', default='greenhouse,lever,ashby')
    ap.add_argument('--suffix', default='FINAL', help='slug file suffix (FINAL or NEW)')
    args = ap.parse_args()

    tasks = []
    for ats in args.ats.split(','):
        path = f'{args.slug_dir}/cc_{ats}_{args.suffix}.txt'
        try:
            slugs = [ln.strip() for ln in open(path) if ln.strip()]
        except FileNotFoundError:
            print(f'WARN: no slug file for {ats} at {path}', file=sys.stderr); continue
        if args.limit: slugs = slugs[:args.limit]
        for s in slugs: tasks.append((ats, s))

    print(f'tasks: {len(tasks)} total, workers={args.workers}', file=sys.stderr)
    ok = err = jobs_total = 0
    by_ats = {}
    with open(args.out, 'w') as out_f, ThreadPoolExecutor(args.workers) as ex:
        futs = [ex.submit(run_adapter, ats, slug) for ats, slug in tasks]
        for i, f in enumerate(as_completed(futs), 1):
            ats, slug, jobs, err_msg = f.result()
            by_ats.setdefault(ats, {'tenants_ok':0, 'tenants_err':0, 'jobs':0})
            if err_msg:
                err += 1; by_ats[ats]['tenants_err'] += 1
                if by_ats[ats]['tenants_err'] <= 10: print(f'  ERR {ats}:{slug} → {err_msg}', file=sys.stderr)
            else:
                ok += 1; by_ats[ats]['tenants_ok'] += 1
                for jp in jobs:
                    out_f.write(json.dumps(asdict(jp)) + '\n')
                    jobs_total += 1
                    by_ats[ats]['jobs'] += 1
            if i % 50 == 0 or i == len(tasks):
                print(f'  progress {i}/{len(tasks)} ok={ok} err={err} jobs={jobs_total}', file=sys.stderr)

    print(f'\nDONE: {ok} tenants ok, {err} tenants err, {jobs_total} jobs written to {args.out}')
    print(f'{"ATS":<16}{"ok":>6}{"err":>6}{"jobs":>10}')
    for ats, s in by_ats.items():
        print(f'{ats:<16}{s["tenants_ok"]:>6}{s["tenants_err"]:>6}{s["jobs"]:>10}')

if __name__ == '__main__':
    main()
