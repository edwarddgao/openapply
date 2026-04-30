#!/usr/bin/env python3
"""Launch Codex subagents against the latest OpenApply shortlist.

The script reads shortlists/latest.json, skips roles already recorded in the
application ledger, and starts one non-interactive Codex process per selected
role. Each Codex process receives a unique agent-browser session name; the
launcher closes that session after the process exits.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILL_PATH = Path.home() / '.agents/skills/job-applications/SKILL.md'
DEFAULT_APPLICANT_PROFILE = Path(os.environ.get('OPENAPPLY_APPLICANT_PROFILE', REPO_ROOT / 'config/applicant-profile.json'))
DEFAULT_APPLICATION_POLICY = Path(os.environ.get('OPENAPPLY_APPLICATION_POLICY', REPO_ROOT / 'config/application-policy.json'))
DEFAULT_OUTPUT_DIR = Path(os.environ.get('OPENAPPLY_CODEX_OUTPUT_DIR', REPO_ROOT / 'applications/codex-subagent-logs'))
DEFAULT_STATUS_PATH = Path('applications/status.jsonl')
SKIP_STATUSES = {'submitted', 'blocked', 'needs_input'}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def repo_path(repo: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo / path


def ledger_path(repo: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo))
    except ValueError:
        return str(path)


def load_latest(repo: Path, latest_path: Path) -> dict[str, Any]:
    path = repo_path(repo, latest_path)
    if path.exists():
        return json.loads(path.read_text(encoding='ascii'))

    summaries = sorted((repo / 'shortlists').glob('date=*/summary.json'))
    if not summaries:
        raise SystemExit(f'No latest pointer or date-scoped summaries found under {repo / "shortlists"}')
    summary = json.loads(summaries[-1].read_text(encoding='ascii'))
    return {
        'date': summary['date'],
        'fresh': summary['fresh_csv'],
        'backlog': summary['backlog_csv'],
        'removed': summary['removed_csv'],
        'summary': str(summaries[-1]),
        'report': summary.get('report_md', ''),
    }


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline='', encoding='ascii') as file:
        return list(csv.DictReader(file))


def load_statuses(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    statuses: dict[str, dict[str, Any]] = {}
    for lineno, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            print(f'warning: skipping malformed status line {path}:{lineno}: {exc}', file=sys.stderr)
            continue
        role_id = item.get('id')
        if role_id:
            statuses[role_id] = item
    return statuses


def validate_json_file(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f'{label} not found: {path}')
    try:
        json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise SystemExit(f'{label} is not valid JSON: {path}: {exc}') from exc


def run_agent_browser(repo: Path, command: list[str], log_file: Any, timeout: int = 60) -> str:
    display = ' '.join(str(part) for part in command)
    log_file.write(f'\n$ {display}\n')
    log_file.flush()
    proc = subprocess.run(
        command,
        cwd=repo,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    log_file.write(proc.stdout)
    log_file.flush()
    if proc.returncode != 0:
        raise RuntimeError(f'agent-browser exited with code {proc.returncode}: {proc.stdout.strip()}')
    return proc.stdout


def prepare_browser_session(row: dict[str, str], session: str, args: argparse.Namespace, log_file: Any) -> None:
    close_browser_session(args.repo, session)
    run_agent_browser(
        args.repo,
        [
            'agent-browser',
            '--session', session,
            '--headed',
            '--args', '--disable-blink-features=AutomationControlled',
            'open', row.get('apply_url', ''),
        ],
        log_file,
        timeout=120,
    )
    run_agent_browser(args.repo, ['agent-browser', '--session', session, 'wait', '--load', 'networkidle'], log_file)
    if args.browser_wait_ms > 0:
        run_agent_browser(args.repo, ['agent-browser', '--session', session, 'wait', str(args.browser_wait_ms)], log_file)


def append_status(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as file:
        file.write(json.dumps(record, sort_keys=True) + '\n')


def parse_status(message: str) -> tuple[str, str]:
    text = ' '.join(message.strip().split())
    lower = text.lower()
    if lower.startswith('submitted'):
        return 'submitted', text
    if lower.startswith('blocked'):
        return 'blocked', text
    if lower.startswith('needs input') or lower.startswith('needs_input'):
        return 'needs_input', text
    return 'unknown', text or 'no final message'


def safe_slug(value: str, fallback: str = 'role') -> str:
    value = re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')
    return value[:36] or fallback


def session_name(row: dict[str, str], index: int) -> str:
    digest = hashlib.sha1((row.get('id') or row.get('apply_url') or str(index)).encode('utf-8')).hexdigest()[:8]
    return f'codex-{index}-{safe_slug(row.get("company_slug", "company"), "company")}-{digest}'


def build_prompt(row: dict[str, str], session: str, args: argparse.Namespace, snapshot_date: str) -> str:
    if args.prepare_browser:
        browser_instructions = f"""- Use the already-prepared agent-browser session named {session}; the launcher has already opened the application URL in a headed browser session.
- Do not close, reopen, relaunch, or change browser launch flags before inspecting the page.
- Fill and verify the form manually using the applicant profile and policy before submitting."""
    else:
        browser_instructions = f"""- Use agent-browser for browser automation with unique session name: {session}.
- Open the application URL yourself.
- Verify all filled values before submitting."""

    return f"""You are an external Codex subagent for job applications. Apply to exactly one role.

Role:
- Snapshot date: {snapshot_date}
- ID: {row.get('id', '')}
- Company: {row.get('company_slug', '')}
- Title: {row.get('title', '')}
- Location: {row.get('locations', '')}
- ATS/source: {row.get('source', '')}
- Apply URL: {row.get('apply_url', '')}

Required workflow:
- Before interacting with the application, read and follow {args.skill_path}.
{browser_instructions}
- Do not use browser extensions, third-party autofill helpers, or AI autofill tools. Fill forms directly with agent-browser from the applicant profile and policy.
- Close the agent-browser session when done if possible. The launcher will also attempt cleanup.

Applicant data:
- Read applicant facts and commitments from {args.applicant_profile}. This JSON profile is the source of truth for contact details, resume path, work authorization, sponsorship, availability, preferences, clearances, screening facts, demographics preferences, education, and languages.
- Read reusable answer strategy from {args.application_policy}. Use it to apply profile facts to recurring form questions such as referral source, cover letters, in-office commitments, clearance, export-control status, compensation, transcript availability, and negative/conflict screening.
- Do not print or dump the full applicant profile or application policy into logs or the final message.
- If the profile and policy conflict on a factual answer, prefer the applicant profile. If a required answer is missing from both files, the resume, and visible application context, stop and return Needs input with the exact question text.

Rules:
- Do not apply to any other role.
- Do not invent facts.
- Submit only if all required answers are known.
- If a required answer cannot be inferred, stop and return Needs input with exact question text.
- If blocked, return the exact blocker.
- Final response must be one concise status line only: Submitted / Blocked / Needs input, with confirmation URL or exact blocker.
"""


def close_browser_session(repo: Path, session: str) -> None:
    subprocess.run(
        ['agent-browser', '--session', session, 'close'],
        cwd=repo,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def run_codex(row: dict[str, str], index: int, args: argparse.Namespace, snapshot_date: str) -> dict[str, Any]:
    session = session_name(row, index)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        output_dir.chmod(0o700)
    except OSError:
        pass

    stem = f'{index:03d}-{safe_slug(row.get("company_slug", "company"), "company")}-{safe_slug(row.get("title", "role"), "role")}'
    final_path = output_dir / f'{stem}.txt'
    log_path = output_dir / f'{stem}.log'
    prompt = build_prompt(row, session, args, snapshot_date)
    final_path.unlink(missing_ok=True)

    cmd = [
        'codex', 'exec',
        '-C', str(args.repo),
        '-s', args.sandbox,
        '--output-last-message', str(final_path),
    ]
    if args.reasoning_effort:
        cmd.extend(['-c', f'model_reasoning_effort="{args.reasoning_effort}"'])
    if args.model:
        cmd.extend(['--model', args.model])
    cmd.append(prompt)

    started_at = utc_now()
    return_code = None
    try:
        with log_path.open('w', encoding='utf-8') as log_file:
            if args.prepare_browser:
                prepare_browser_session(row, session, args, log_file)
                log_file.write(f'\nPrepared browser session {session}\n')
                log_file.flush()
            proc = subprocess.run(
                cmd,
                cwd=args.repo,
                env=os.environ.copy(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=args.timeout_minutes * 60,
                check=False,
            )
            return_code = proc.returncode
    except subprocess.TimeoutExpired:
        return_code = 124
    except (RuntimeError, OSError) as exc:
        return_code = 1
        final_path.write_text(f'Blocked: browser setup failed: {exc}', encoding='utf-8')
    finally:
        close_browser_session(args.repo, session)

    final_message = ''
    if final_path.exists():
        final_message = final_path.read_text(encoding='utf-8', errors='replace').strip()
    elif log_path.exists():
        data = log_path.read_text(encoding='utf-8', errors='replace')
        final_message = data[-4000:].strip()

    status, detail = parse_status(final_message)
    if return_code not in {0, None} and status == 'unknown':
        detail = f'Codex exited with code {return_code}: {detail}'

    record = {
        'id': row.get('id', ''),
        'status': status,
        'detail': detail,
        'company_slug': row.get('company_slug', ''),
        'title': row.get('title', ''),
        'apply_url': row.get('apply_url', ''),
        'snapshot_date': snapshot_date,
        'session': session,
        'codex_return_code': return_code,
        'output_path': ledger_path(args.repo, final_path),
        'log_path': ledger_path(args.repo, log_path),
        'started_at': started_at,
        'updated_at': utc_now(),
    }
    return record


def selected_rows(rows: list[dict[str, str]], statuses: dict[str, dict[str, Any]], args: argparse.Namespace) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    retry_statuses = {part.strip() for part in args.retry_status.split(',') if part.strip()}
    only_ids = {part.strip() for part in args.only_id.split(',') if part.strip()}

    for row in rows:
        role_id = row.get('id', '')
        if only_ids and role_id not in only_ids:
            continue
        if not row.get('apply_url'):
            continue
        previous = statuses.get(role_id)
        if previous:
            previous_status = previous.get('status')
            if previous_status in SKIP_STATUSES and previous_status not in retry_statuses:
                continue
        selected.append(row)
        if args.limit and len(selected) >= args.limit:
            break
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply to latest shortlist roles with Codex subagents')
    parser.add_argument('--repo', type=Path, default=REPO_ROOT)
    parser.add_argument('--latest', type=Path, default=Path('shortlists/latest.json'))
    parser.add_argument('--pool', choices=['fresh', 'backlog', 'both'], default='fresh')
    parser.add_argument('--limit', type=int, default=1)
    parser.add_argument('--parallel', type=int, default=1)
    parser.add_argument('--status', type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument('--applicant-profile', type=Path, default=DEFAULT_APPLICANT_PROFILE)
    parser.add_argument('--application-policy', type=Path, default=DEFAULT_APPLICATION_POLICY)
    parser.add_argument('--skill-path', type=Path, default=DEFAULT_SKILL_PATH)
    parser.add_argument('--prepare-browser', dest='prepare_browser', action='store_true')
    parser.add_argument('--no-prepare-browser', dest='prepare_browser', action='store_false')
    parser.set_defaults(prepare_browser=True)
    parser.add_argument('--browser-wait-ms', type=int, default=3000)
    parser.add_argument('--sandbox', default='danger-full-access', choices=['read-only', 'workspace-write', 'danger-full-access'])
    parser.add_argument('--model', default='')
    parser.add_argument('--reasoning-effort', default='low')
    parser.add_argument('--timeout-minutes', type=int, default=45)
    parser.add_argument('--retry-status', default='', help='comma-separated statuses to retry, e.g. needs_input,blocked')
    parser.add_argument('--only-id', default='', help='comma-separated role ids to apply regardless of order')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    args.repo = args.repo.resolve()
    args.status = repo_path(args.repo, args.status)
    args.output_dir = repo_path(args.repo, args.output_dir.expanduser())
    args.applicant_profile = repo_path(args.repo, args.applicant_profile.expanduser())
    args.application_policy = repo_path(args.repo, args.application_policy.expanduser())
    args.skill_path = args.skill_path.expanduser()

    if not args.dry_run:
        if not shutil.which('codex'):
            raise SystemExit('codex CLI not found on PATH')
        if not shutil.which('agent-browser'):
            raise SystemExit('agent-browser CLI not found on PATH')
        validate_json_file(args.applicant_profile, 'Applicant profile')
        validate_json_file(args.application_policy, 'Application policy')

    latest = load_latest(args.repo, args.latest)
    paths = []
    if args.pool in {'fresh', 'both'}:
        paths.append(repo_path(args.repo, latest['fresh']))
    if args.pool in {'backlog', 'both'}:
        paths.append(repo_path(args.repo, latest['backlog']))

    rows: list[dict[str, str]] = []
    for path in paths:
        rows.extend(read_rows(path))

    statuses = load_statuses(args.status)
    todo = selected_rows(rows, statuses, args)
    if args.dry_run:
        print(json.dumps({
            'date': latest['date'],
            'pool': args.pool,
            'selected': [
                {
                    'id': row.get('id'),
                    'company_slug': row.get('company_slug'),
                    'title': row.get('title'),
                    'apply_url': row.get('apply_url'),
                }
                for row in todo
            ],
        }, indent=2))
        return

    if not todo:
        print('No eligible roles selected')
        return

    max_workers = max(1, min(args.parallel, len(todo)))
    print(f'Launching {len(todo)} role(s) from {latest["date"]} with parallel={max_workers}', flush=True)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for index, row in enumerate(todo, 1):
            print(f'Starting {index}/{len(todo)}: {row.get("company_slug", "")} - {row.get("title", "")}', flush=True)
            futures[executor.submit(run_codex, row, index, args, latest['date'])] = row
        for future in as_completed(futures):
            record = future.result()
            append_status(args.status, record)
            print(f'Finished: {record["detail"]}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
