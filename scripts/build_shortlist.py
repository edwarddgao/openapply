#!/usr/bin/env python3
"""Build a reusable job shortlist from a local Open-Apply snapshot.

Example:
  python scripts/build_shortlist.py --date latest --fresh-days 60

Outputs:
  shortlists/date=YYYY-MM-DD/fresh.csv
  shortlists/date=YYYY-MM-DD/backlog.csv
  shortlists/date=YYYY-MM-DD/removed.csv
  shortlists/date=YYYY-MM-DD/summary.json
  shortlists/date=YYYY-MM-DD/report.md
"""
import argparse
import csv
import html
import json
import re
import unicodedata
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq


BACKLOG_DAYS = 120


ROLE_RE = re.compile(r'''(?ix)\b(
  software|developer|programmer|frontend|front[- ]?end|backend|back[- ]?end|
  full[- ]?stack|fullstack|web\s+engineer|mobile\s+engineer|ios\s+engineer|
  android\s+engineer|firmware|embedded|cloud\s+engineer|platform\s+engineer|
  infrastructure\s+engineer|devops|site\s+reliability|sre\b|systems\s+engineer|
  qa\s+engineer|test\s+engineer|automation\s+engineer|
  data\s+(engineer|analyst|scientist|platform|infrastructure)|analytics\s+engineer|
  business\s+intelligence|bi\s+engineer|machine\s+learning|ml\b|ai\s+engineer|
  artificial\s+intelligence|research\s+engineer|bioinformatics|computational\s+biologist|
  quantitative\s+(developer|researcher|analyst|engineer)
)\b''')

ENTRY_TITLE_RE = re.compile(r'''(?ix)\b(
  entry[- ]?level|junior|jr\.?|new\s+grad|new\s+graduate|recent\s+graduate|
  graduate|early\s+career|university\s+grad|associate\s+(software|data|analytics|machine\s+learning|ml|ai|engineer|developer)|
  software\s+engineer\s+(i|1)\b|data\s+(engineer|analyst|scientist)\s+(i|1)\b|
  engineer\s+(i|1)\b|developer\s+(i|1)\b|analyst\s+(i|1)\b
)\b''')

ENTRY_DESC_RE = re.compile(r'''(?ix)\b(
  entry[- ]?level|junior|new\s+grad|new\s+graduate|recent\s+graduate|
  early\s+career|university\s+grad|0\s*[-+]\s*2\s+years|0\s*[-+]\s*3\s+years|
  0\s*to\s*2\s+years|0\s*to\s*3\s+years|less\s+than\s+2\s+years|
  no\s+prior\s+experience|1\+?\s+years?\s+(of\s+)?experience
)\b''')

MID_TITLE_RE = re.compile(r'''(?ix)\b(
  intermediate|mid[- ]?level|software\s+engineer\s+(ii|2)\b|
  data\s+(engineer|analyst|scientist)\s+(ii|2)\b|engineer\s+(ii|2)\b|
  developer\s+(ii|2)\b|analyst\s+(ii|2)\b|sde\s*(ii|2)\b|level\s*2\b|l2\b
)\b''')

LIKELY_MID_TITLE_RE = re.compile(r'''(?ix)\b(
  software\s+engineer\s+(iii|3)\b|data\s+(engineer|analyst|scientist)\s+(iii|3)\b|
  engineer\s+(iii|3)\b|developer\s+(iii|3)\b|analyst\s+(iii|3)\b|
  sde\s*(iii|3)\b|level\s*3\b|l3\b
)\b''')

SENIOR_TITLE_RE = re.compile(r'\b(senior|sr\.?|staff|principal|lead|manager|director|head of|vp|vice president|architect)\b', re.I)
INTERN_TITLE_RE = re.compile(r'\b(intern|internship|co[- ]?op|coop|working student|student researcher)\b', re.I)

NONTECH_TITLE_RE = re.compile(r'''(?ix)\b(
  account\s+executive|sales\s+engineer|solutions?\s+engineer|solution\s+consultant|
  customer\s+success|product\s+owner|product\s+manager|designer|marketing|growth|
  operations\s+associate|special\s+ops|recruit|talent|people\s+partner|finance|
  billing|credit\s+analyst|investment\s+analyst|clinical|nurse|physician|
  recipe\s+developer|raw\s+materials\s+developer|materials\s+developer|
  curriculum\s+developer|instructional\s+developer|content\s+developer|business\s+development
)\b''')

HARDWARE_ONLY_TITLE_RE = re.compile(
    r'\b(mechanical\s+engineer|electrical\s+engineer|manufacturing\s+engineer|field\s+engineer|civil\s+engineer|structural\s+engineer)\b',
    re.I,
)

US_CA_RE = re.compile(r'''(?ix)\b(
  united\s+states|usa|u\.s\.|canada|canadian|north\s+america|
  toronto|vancouver|montreal|waterloo|ottawa|calgary|edmonton|kanata|kitchener|winnipeg|
  new\s+york|nyc|san\s+francisco|bay\s+area|seattle|boston|austin|chicago|los\s+angeles|
  denver|atlanta|miami|dallas|houston|san\s+diego|washington\s+dc|palo\s+alto|
  mountain\s+view|menlo\s+park|san\s+jose|philadelphia|baltimore|brooklyn|pittsburgh|
  raleigh|indianapolis|plano|redmond|bellevue|sunnyvale|santa\s+clara|
  alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|
  georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|
  maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|
  nevada|new\s+hampshire|new\s+jersey|new\s+mexico|north\s+carolina|north\s+dakota|
  ohio|oklahoma|oregon|pennsylvania|rhode\s+island|south\s+carolina|south\s+dakota|
  tennessee|texas|utah|vermont|virginia|washington|west\s+virginia|wisconsin|wyoming|
  ontario|british\s+columbia|quebec|alberta|manitoba|saskatchewan|nova\s+scotia|
  new\s+brunswick|newfoundland|prince\s+edward\s+island
)\b''')

US_CA_CODE_RE = re.compile(
    r'(?:^|[\s,;(/-])(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY|ON|BC|QC|AB|MB|SK|NS|NB|NL|PE)(?:$|[\s,;)/-])'
)

REGION_ONLY_RE = re.compile(r'''(?ix)^\s*(united\s+states(\s+of\s+america)?|usa|u\.s\.|us|canada|north\s+america|americas?)\s*$''')
REMOTE_RE = re.compile(r'\b(remote|distributed|anywhere|global|worldwide|home based|work from home|wfh)\b', re.I)
HYBRID_RE = re.compile(r'\b(hybrid|office|onsite|on-site|in office|hq|headquarters)\b', re.I)

FOREIGN_RE = re.compile(r'''(?ix)\b(
  latin\s+america|latam|mexico|europe|emea|united\s+kingdom|uk|england|scotland|ireland|
  london|bristol|dublin|spain|madrid|barcelona|france|paris|germany|berlin|munich|
  italy|rome|milan|netherlands|amsterdam|poland|warsaw|romania|portugal|lisbon|
  sweden|stockholm|austria|vienna|denmark|copenhagen|aarhus|serbia|belgrade|armenia|
  kazakhstan|ukraine|india|bengaluru|bangalore|delhi|mumbai|hyderabad|singapore|
  australia|sydney|melbourne|new\s+zealand|japan|tokyo|korea|seoul|china|beijing|
  shanghai|philippines|vietnam|thailand|indonesia|malaysia|israel|tel\s+aviv|turkey|
  uae|dubai|brazil|argentina|colombia|chile|peru|south\s+africa
)\b''')

SECURITY_RE = re.compile(r'''(?ix)\b(
  security\s+clearance|secret\s+clearance|top\s+secret|ts/sci|clearance\s+required|
  active\s+clearance|eligible\s+for\s+clearance|\bitar\b|u\.s\.\s+person|us\s+person|
  u\.s\.\s+citizen|us\s+citizen|u\.s\.\s+citizenship|us\s+citizenship|
  citizenship\s+is\s+required|public\s+trust
)\b''')

SECURITY_TITLE_RE = re.compile(r'(?ix)(\bsecret\b|top\s+secret|ts/sci|\bsci\b|clearance|public\s+trust|\bitar\b|u\.s\.\s+person|us\s+person|u\.s\.\s+citizen|us\s+citizen|citizenship)')
SPONSOR_RE = re.compile(r'\b(visa sponsorship|sponsorship|will not sponsor|do not sponsor|unable to sponsor|without sponsorship|h-?1b|h1b|tn visa|tn status)\b', re.I)
TN_POSITIVE_RE = re.compile(r'\b(tn visa|tn status|visa sponsorship available|sponsor visas|sponsorship available|h-?1b transfer)\b', re.I)
FEDERAL_RE = re.compile(r'\b(federal|dod|department of defense|army|navy|air force|space force|government|public sector|homeland security|intelligence community|cia|nsa|fbi|defense|aerospace|military)\b', re.I)

TAG_RE = re.compile(r'<[^>]+>')
WS_RE = re.compile(r'\s+')
YEAR_RANGE_RE = re.compile(r'\b([0-9]{1,2})\s*(?:-|to)\s*([0-9]{1,2})\s*\+?\s+years?\b', re.I)
YEAR_SINGLE_RE = re.compile(r'\b(?:(minimum\s+of|at\s+least)\s+)?([0-9]{1,2})\s*(?:\+|\s+\+)?\s+years?\b', re.I)
REQ_CONTEXT_RE = re.compile(r'''(?ix)\b(
  experience|experienced|qualification|qualifications|required|required qualifications|requirements|
  minimum|preferred|you have|must have|background|professional|industry|working with|
  building|developing|designing|hands-on|proficiency|expertise|track record|proven
)\b''')
BAD_YEAR_CONTEXT_RE = re.compile(r'(?ix)\b(years\s+ago|founded|history|legacy|anniversary|[0-9]{1,2}\s+years\s+old)\b')


def clean(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, (list, tuple)):
        value = '; '.join(str(v) for v in value if v is not None)
    value = html.unescape(str(value)).replace('\xa0', ' ')
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    return WS_RE.sub(' ', value).strip()


def strip_html(value: Any) -> str:
    return clean(TAG_RE.sub(' ', str(value or '')))


def latest_date(data_dir: Path) -> str:
    dates = sorted(p.name.removeprefix('date=') for p in data_dir.glob('date=*') if p.is_dir())
    if not dates:
        raise SystemExit(f'No date partitions found under {data_dir}')
    return dates[-1]


def parse_posted_date(value: str) -> date | None:
    if not value:
        return None
    text = value.replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        return dt.date()
    except ValueError:
        try:
            return datetime.strptime(text[:10], '%Y-%m-%d').date()
        except ValueError:
            return None


def age_days(posted_at: str, snapshot_date: date) -> int | None:
    posted = parse_posted_date(posted_at)
    if posted is None:
        return None
    return max(0, (snapshot_date - posted).days)


def age_bucket(age: int | None) -> str:
    if age is None:
        return 'unknown'
    if age <= 7:
        return '0-7d'
    if age <= 14:
        return '8-14d'
    if age <= 30:
        return '15-30d'
    if age <= 60:
        return '31-60d'
    if age <= 120:
        return '61-120d'
    if age <= 365:
        return '121-365d'
    return '365d+'


def role_type(title: str, department: str) -> str:
    text = f'{title} {department}'.lower()
    if re.search(r'\b(data scientist|machine learning|ml\b|ai engineer|research engineer|bioinformatics|computational|quantitative)\b', text):
        return 'Data/ML'
    if re.search(r'\b(data engineer|data analyst|analytics engineer|business intelligence|bi engineer|data platform)\b', text):
        return 'Data/Analytics'
    if re.search(r'\b(platform|infrastructure|devops|site reliability|sre|systems engineer|cloud engineer)\b', text):
        return 'Platform/Infra'
    if re.search(r'\b(test engineer|qa engineer|qa automation|automation engineer)\b', text):
        return 'Test/QA'
    if re.search(r'\b(firmware|embedded)\b', text):
        return 'Embedded/Firmware'
    if re.search(r'\b(front|backend|full[- ]?stack|fullstack|web|mobile|ios|android|developer|programmer|software)\b', text):
        return 'Software'
    return 'Software/Data'


def location_bucket(locations: str, remote: bool) -> str:
    if remote or REMOTE_RE.search(locations) or REGION_ONLY_RE.match(locations):
        return 'remote-or-region'
    has_us_ca = bool(US_CA_RE.search(locations) or US_CA_CODE_RE.search(locations))
    has_foreign = bool(FOREIGN_RE.search(locations))
    if has_foreign and not has_us_ca:
        return 'foreign-or-unknown'
    if has_us_ca and has_foreign:
        return 'mixed-us-ca-foreign-onsite'
    if has_us_ca and HYBRID_RE.search(locations):
        return 'hybrid-us-ca'
    if has_us_ca:
        return 'onsite-us-ca'
    return 'foreign-or-unknown'


def salary_text(row: dict[str, Any]) -> str:
    if row.get('salary_min') is None and row.get('salary_max') is None:
        return ''

    def fmt(value: Any) -> str:
        if value is None:
            return ''
        try:
            value = float(value)
        except (TypeError, ValueError):
            return clean(value)
        return f'{int(value):,}' if value.is_integer() else f'{value:,.0f}'

    low = fmt(row.get('salary_min'))
    high = fmt(row.get('salary_max'))
    amount = low if not high or low == high else f'{low}-{high}'
    return ' '.join(part for part in [clean(row.get('salary_currency')), amount, clean(row.get('salary_period'))] if part)


def valid_year_context(text: str, start: int, end: int) -> bool:
    ctx = text[max(0, start - 220):min(len(text), end + 320)]
    if BAD_YEAR_CONTEXT_RE.search(ctx) and not re.search(r'(?i)years?\s+of\s+(relevant\s+)?experience', ctx):
        return False
    return bool(REQ_CONTEXT_RE.search(ctx))


def year_matches(text: str) -> list[dict[str, Any]]:
    matches = []
    for match in YEAR_RANGE_RE.finditer(text):
        matches.append({
            'start': match.start(), 'end': match.end(), 'text': match.group(0),
            'lo': int(match.group(1)), 'hi': int(match.group(2)), 'kind': 'range',
            'valid': valid_year_context(text, match.start(), match.end()),
        })
    occupied = [(m['start'], m['end']) for m in matches]
    for match in YEAR_SINGLE_RE.finditer(text):
        if any(not (match.end() <= start or match.start() >= end) for start, end in occupied):
            continue
        years = int(match.group(2))
        matches.append({
            'start': match.start(), 'end': match.end(), 'text': match.group(0),
            'lo': years, 'hi': years, 'kind': 'single-plus' if ('+' in match.group(0) or match.group(1)) else 'single',
            'valid': valid_year_context(text, match.start(), match.end()),
        })
    return sorted(matches, key=lambda m: m['start'])


def senior_evidence_type(match: dict[str, Any]) -> str:
    lo, hi = match['lo'], match['hi']
    if not match['valid']:
        return 'nonrequirement-year-mention'
    if match['kind'] in {'single-plus', 'single'} and lo >= 5:
        return 'strict-5plus'
    if match['kind'] == 'range' and lo >= 5:
        return 'strict-range-starts-5plus'
    if match['kind'] == 'range' and lo >= 4 and hi >= 6:
        return 'senior-leaning-range'
    if match['kind'] == 'range' and hi >= 6:
        return 'wide-mid-to-senior-range'
    if match['kind'] == 'range' and hi == 5:
        return 'borderline-3to5-ish'
    if hi >= 5:
        return 'other-5plus-signal'
    return 'not-senior-year-signal'


SENIOR_DESCRIPTION_TYPES = {'strict-5plus', 'strict-range-starts-5plus', 'wide-mid-to-senior-range', 'senior-leaning-range'}


def choose_senior_evidence(matches: list[dict[str, Any]]) -> tuple[str, str]:
    candidates = [match for match in matches if match['hi'] >= 5]
    if not candidates:
        return '', ''
    priority = {
        'strict-5plus': 0,
        'strict-range-starts-5plus': 1,
        'senior-leaning-range': 2,
        'wide-mid-to-senior-range': 3,
        'borderline-3to5-ish': 4,
        'other-5plus-signal': 5,
        'nonrequirement-year-mention': 8,
    }
    best = min(candidates, key=lambda match: priority.get(senior_evidence_type(match), 99))
    return senior_evidence_type(best), best['text']


def seniority_bucket(title: str, full_text: str, senior_type: str, matches: list[dict[str, Any]]) -> str:
    valid_years = [m for m in matches if m['valid']]
    max_year = max((m['hi'] for m in valid_years), default=None)
    min_year = min((m['lo'] for m in valid_years), default=None)
    if SENIOR_TITLE_RE.search(title):
        return 'senior-title'
    if senior_type in SENIOR_DESCRIPTION_TYPES:
        return 'senior-description'
    if ENTRY_TITLE_RE.search(title):
        return 'explicit-entry-title'
    if ENTRY_DESC_RE.search(full_text):
        return 'likely-entry-description'
    if MID_TITLE_RE.search(title):
        return 'mid-possible-title'
    if LIKELY_MID_TITLE_RE.search(title):
        return 'likely-mid-title'
    if max_year is not None:
        if max_year <= 3:
            return 'possible-entry-years'
        if min_year is not None and min_year <= 2 and max_year <= 4:
            return 'mid-possible-years'
        if max_year <= 5:
            return 'mid-possible-years'
    return 'ambiguous-no-years'


def sponsorship_note(description: str) -> str:
    if TN_POSITIVE_RE.search(description):
        return 'mentions-visa/TN'
    if SPONSOR_RE.search(description):
        return 'check-sponsorship-language'
    return 'no-obvious-blocker'


def read_jobs(data_dir: Path, snapshot_date: str) -> Iterable[dict[str, Any]]:
    paths = sorted((data_dir / f'date={snapshot_date}').glob('source=*/*.parquet'))
    if not paths:
        raise SystemExit(f'No parquet files found for date={snapshot_date} under {data_dir}')
    columns = [
        'id', 'source', 'source_slug', 'title', 'apply_url', 'description_html', 'employment_type',
        'department', 'locations', 'remote', 'posted_at', 'salary_min', 'salary_max',
        'salary_currency', 'salary_period',
    ]
    for path in paths:
        table = pq.read_table(path, columns=columns)
        yield from table.to_pylist()


def normalize(row: dict[str, Any], snapshot_date: date) -> dict[str, Any]:
    title = clean(row.get('title'))
    department = clean(row.get('department'))
    locations = clean(row.get('locations') or [])
    description = strip_html(row.get('description_html'))
    posted_at = clean(row.get('posted_at'))
    age = age_days(posted_at, snapshot_date)
    full_text = f'{title} {department} {description}'
    matches = year_matches(full_text)
    senior_type, senior_match = choose_senior_evidence(matches)
    return {
        'id': clean(row.get('id')),
        'source': clean(row.get('source')),
        'company_slug': clean(row.get('source_slug')),
        'title': title,
        'role_type': role_type(title, department),
        'seniority_bucket': seniority_bucket(title, full_text, senior_type, matches),
        'locations': locations,
        'location_bucket': location_bucket(locations, bool(row.get('remote'))),
        'posted_at': posted_at,
        'age_days': '' if age is None else age,
        'age_bucket': age_bucket(age),
        'salary': salary_text(row),
        'sponsorship_note': sponsorship_note(description),
        'federal_defense_note': 'check-federal/defense' if FEDERAL_RE.search(full_text) else '',
        'senior_evidence_type': senior_type,
        'senior_evidence_match': senior_match,
        'apply_url': clean(row.get('apply_url')),
        'department': department,
        'employment_type': clean(row.get('employment_type')),
        'snippet': description[:240],
        '_full_text': full_text,
    }


def removal_reasons(job: dict[str, Any], excluded_role_types: set[str], backlog_days: int) -> list[str]:
    reasons = []
    title = job['title']
    text = job['_full_text']
    if job['role_type'] in excluded_role_types:
        reasons.append(f'excluded-role:{job["role_type"]}')
    if SENIOR_TITLE_RE.search(title):
        reasons.append('senior-title')
    if INTERN_TITLE_RE.search(title):
        reasons.append('intern-title')
    if NONTECH_TITLE_RE.search(title):
        reasons.append('nontech-title')
    if HARDWARE_ONLY_TITLE_RE.search(title) and not re.search(r'\bsoftware|firmware|embedded|data|automation\b', title, re.I):
        reasons.append('hardware-only-title')
    if job['location_bucket'] in {'remote-or-region', 'foreign-or-unknown'}:
        reasons.append(job['location_bucket'])
    if SECURITY_RE.search(text) or SECURITY_TITLE_RE.search(f'{title} {job["department"]}'):
        reasons.append('security-clearance-citizenship')
    if job['senior_evidence_type'] in SENIOR_DESCRIPTION_TYPES:
        reasons.append('senior-description')
    if job['age_days'] == '':
        reasons.append('missing-posted-at')
    elif int(job['age_days']) > backlog_days:
        reasons.append(f'stale>{backlog_days}d')
    return reasons


def write_csv(path: Path, rows: list[dict[str, Any]], row_name: str) -> None:
    fields = [
        row_name, 'id', 'source', 'company_slug', 'title', 'role_type', 'seniority_bucket',
        'locations', 'location_bucket', 'posted_at', 'age_days', 'age_bucket', 'salary',
        'sponsorship_note', 'federal_defense_note', 'senior_evidence_type', 'senior_evidence_match',
        'remove_reasons', 'apply_url', 'department', 'employment_type', 'snippet',
    ]
    with path.open('w', newline='', encoding='ascii') as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for index, row in enumerate(rows, 1):
            out = dict(row)
            out[row_name] = index
            out.pop('_full_text', None)
            writer.writerow(out)


def counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    return dict(Counter(row.get(field) or 'not flagged' for row in rows))


def write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        f'# Shortlist - {summary["date"]}',
        '',
        f'- Fresh CSV: `{summary["fresh_csv"]}`',
        f'- Backlog CSV: `{summary["backlog_csv"]}`',
        f'- Removed CSV: `{summary["removed_csv"]}`',
        '',
        '## Rules',
        '',
        f'- Fresh: age <= {summary["fresh_days"]} days',
        f'- Backlog: {summary["fresh_days"] + 1}-{summary["backlog_days"]} days',
        '- Excludes explicit senior/intern titles, clean senior description signals, remote/region-only locations, security/citizenship/ITAR blockers, and configured role types.',
        '',
        '## Result',
        '',
        f'- Source rows scanned: {summary["source_rows"]:,}',
        f'- Role-like rows: {summary["role_like_rows"]:,}',
        f'- Fresh rows: {summary["fresh_rows"]:,}',
        f'- Backlog rows: {summary["backlog_rows"]:,}',
        f'- Removed rows: {summary["removed_rows"]:,}',
        '',
    ]
    for label, key in [
        ('Fresh role types', 'fresh_role_type_counts'),
        ('Fresh seniority buckets', 'fresh_seniority_bucket_counts'),
        ('Fresh age buckets', 'fresh_age_bucket_counts'),
        ('Fresh sponsorship notes', 'fresh_sponsorship_note_counts'),
        ('Fresh federal/defense notes', 'fresh_federal_defense_note_counts'),
        ('Removed reasons', 'removed_reason_counts'),
    ]:
        lines.extend([f'## {label}', ''])
        for name, count in sorted(summary[key].items(), key=lambda item: (-item[1], item[0])):
            lines.append(f'- {name}: {count:,}')
        lines.append('')
    path.write_text('\n'.join(lines), encoding='ascii')


def write_latest_pointer(path: Path, summary: dict[str, Any]) -> None:
    pointer = {
        'date': summary['date'],
        'generated_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        'fresh': summary['fresh_csv'],
        'backlog': summary['backlog_csv'],
        'removed': summary['removed_csv'],
        'summary': summary['summary_json'],
        'report': summary['report_md'],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(pointer, indent=2, sort_keys=True) + '\n', encoding='ascii')


def main() -> None:
    parser = argparse.ArgumentParser(description='Build fresh/backlog job shortlists from local Open-Apply data')
    parser.add_argument('--data-dir', default='data')
    parser.add_argument('--date', default='latest', help='YYYY-MM-DD or latest')
    parser.add_argument('--out-dir', default='shortlists')
    parser.add_argument('--fresh-days', type=int, default=60)
    parser.add_argument('--exclude-role-types', default='Test/QA,Embedded/Firmware')
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    snapshot = latest_date(data_dir) if args.date == 'latest' else args.date
    snapshot_date = datetime.strptime(snapshot, '%Y-%m-%d').date()
    out_dir = Path(args.out_dir) / f'date={snapshot}'
    out_dir.mkdir(parents=True, exist_ok=True)
    excluded_role_types = {part.strip() for part in args.exclude_role_types.split(',') if part.strip()}

    fresh: list[dict[str, Any]] = []
    backlog: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    source_rows = role_like_rows = 0

    for raw in read_jobs(data_dir, snapshot):
        source_rows += 1
        title = clean(raw.get('title'))
        department = clean(raw.get('department'))
        if not ROLE_RE.search(f'{title} {department}'):
            continue
        role_like_rows += 1
        job = normalize(raw, snapshot_date)
        reasons = removal_reasons(job, excluded_role_types, BACKLOG_DAYS)
        if reasons:
            job['remove_reasons'] = ';'.join(reasons)
            removed.append(job)
            continue
        age = int(job['age_days'])
        if age <= args.fresh_days:
            job['remove_reasons'] = ''
            fresh.append(job)
        elif age <= BACKLOG_DAYS:
            job['remove_reasons'] = ''
            backlog.append(job)

    sort_key = lambda row: (row['company_slug'].lower(), row['title'].lower(), row['locations'].lower(), str(row['posted_at']))
    fresh.sort(key=sort_key)
    backlog.sort(key=sort_key)
    removed.sort(key=lambda row: (row.get('remove_reasons', ''),) + sort_key(row))

    fresh_csv = out_dir / 'fresh.csv'
    backlog_csv = out_dir / 'backlog.csv'
    removed_csv = out_dir / 'removed.csv'
    summary_json = out_dir / 'summary.json'
    report_md = out_dir / 'report.md'
    latest_json = Path(args.out_dir) / 'latest.json'
    write_csv(fresh_csv, fresh, 'fresh_row')
    write_csv(backlog_csv, backlog, 'backlog_row')
    write_csv(removed_csv, removed, 'removed_row')

    summary = {
        'date': snapshot,
        'fresh_days': args.fresh_days,
        'backlog_days': BACKLOG_DAYS,
        'excluded_role_types': sorted(excluded_role_types),
        'source_rows': source_rows,
        'role_like_rows': role_like_rows,
        'fresh_rows': len(fresh),
        'backlog_rows': len(backlog),
        'removed_rows': len(removed),
        'fresh_csv': str(fresh_csv),
        'backlog_csv': str(backlog_csv),
        'removed_csv': str(removed_csv),
        'summary_json': str(summary_json),
        'report_md': str(report_md),
        'fresh_role_type_counts': counts(fresh, 'role_type'),
        'fresh_seniority_bucket_counts': counts(fresh, 'seniority_bucket'),
        'fresh_age_bucket_counts': counts(fresh, 'age_bucket'),
        'fresh_sponsorship_note_counts': counts(fresh, 'sponsorship_note'),
        'fresh_federal_defense_note_counts': counts(fresh, 'federal_defense_note'),
        'backlog_role_type_counts': counts(backlog, 'role_type'),
        'removed_reason_counts': dict(Counter(reason for row in removed for reason in row.get('remove_reasons', '').split(';') if reason)),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n', encoding='ascii')
    write_report(report_md, summary)
    write_latest_pointer(latest_json, summary)
    print(json.dumps({
        'fresh_csv': str(fresh_csv),
        'backlog_csv': str(backlog_csv),
        'removed_csv': str(removed_csv),
        'summary_json': str(summary_json),
        'report_md': str(report_md),
        'latest_json': str(latest_json),
        'fresh_rows': len(fresh),
        'backlog_rows': len(backlog),
        'removed_rows': len(removed),
    }, indent=2))


if __name__ == '__main__':
    main()
