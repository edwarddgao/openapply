#!/usr/bin/env python3
"""Launch agent subagents (codex or claude) against the latest OpenApply shortlist.

The script reads shortlists/latest.json, skips roles already recorded in the
application ledger, and starts one non-interactive agent process per selected
role. Each process receives a unique agent-browser session name; the launcher
closes that session after the process exits. Agent backend is chosen via
--agent (codex=`codex exec`, claude=`claude -p`); default is codex.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILL_PATH = REPO_ROOT / 'skills/job-applications/SKILL.md'
DEFAULT_APPLICANT_PROFILE = Path(os.environ.get('OPENAPPLY_APPLICANT_PROFILE', REPO_ROOT / 'config/applicant-profile.json'))
DEFAULT_APPLICATION_POLICY = Path(os.environ.get('OPENAPPLY_APPLICATION_POLICY', REPO_ROOT / 'config/application-policy.json'))
DEFAULT_OUTPUT_DIR = Path(os.environ.get('OPENAPPLY_CODEX_OUTPUT_DIR', REPO_ROOT / 'applications/codex-subagent-logs'))
DEFAULT_STATUS_PATH = Path('applications/status.jsonl')
SKIP_STATUSES = {'submitted', 'blocked', 'needs_input'}
DEFAULT_CHROME_BINARY = os.environ.get(
    'OPENAPPLY_CHROME_BINARY',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
)
DEFAULT_CHROME_SOURCE_PROFILE = os.environ.get(
    'OPENAPPLY_CHROME_SOURCE_PROFILE',
    str(Path.home() / 'Library/Application Support/Google/Chrome'),
)
# Cache/derived dirs to skip when copying the Chrome profile: they bloat the copy
# and carry none of the signed-in-session signal Ashby's reCAPTCHA/SEON score.
CDP_CACHE_EXCLUDES = (
    'Cache/', 'Code Cache/', 'GPUCache/', 'DawnGraphiteCache/', 'DawnWebGPUCache/',
    'GrShaderCache/', 'ShaderCache/', 'Service Worker/CacheStorage/',
    'Service Worker/ScriptCache/', 'Service Worker/Database/',
)
RATE_LIMIT_PHRASES = (
    "out of extra usage",
    "you've hit your usage limit",
    "you have hit your usage limit",
    "you've hit your limit",
    "you have hit your limit",
    "usage limit reached",
    "rate limit",
)
RATE_LIMIT_EVENT = threading.Event()


def is_rate_limited(text: str) -> bool:
    low = (text or '').lower()
    return any(phrase in low for phrase in RATE_LIMIT_PHRASES)


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
    ever_submitted: set[str] = set()
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
            if item.get('status') == 'submitted':
                ever_submitted.add(role_id)
    # Flag roles we've EVER submitted so selected_rows never re-applies, even if a
    # later event (spam-flag retry / 'already applied' / crash) flipped last-status.
    for role_id in ever_submitted:
        statuses[role_id]['_ever_submitted'] = True
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


def _cdp_http(port: int, path: str, method: str = 'GET', timeout: float = 5.0) -> Any:
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode('utf-8')
    return json.loads(body) if body.strip().startswith(('{', '[')) else body


def _cdp_close_other_pages(port: int, keep_id: str) -> None:
    """Close every 'page' target except keep_id.

    agent-browser's `connect` auto-picks a target and does NOT reliably honor a
    specific page's WebSocket URL — with the initial about:blank or the
    Gemini-in-Chrome companion (gemini.google.com/glic) also present, it binds to
    the wrong one. Leaving a single page target makes the bind deterministic.
    Best-effort.
    """
    try:
        targets = _cdp_http(port, '/json')
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return
    for tgt in targets if isinstance(targets, list) else []:
        if tgt.get('type') == 'page' and tgt.get('id') and tgt.get('id') != keep_id:
            try:
                _cdp_http(port, '/json/close/' + tgt['id'])
            except (urllib.error.URLError, OSError, json.JSONDecodeError):
                pass


def start_cdp_chrome(args: argparse.Namespace) -> dict[str, Any]:
    """Copy the real Chrome profile and launch it headed with a CDP port.

    Returns a state dict for teardown. A FULL profile copy (minus caches) launched
    with clean flags carries the live signed-in Google session that Ashby's
    reCAPTCHA/SEON scoring needs; a 3-file copy or anonymous/automation session
    gets spam-flagged. Measured 6/6 across 6 companies on 2026-06-10.

    macOS only as written: the copied cookies are Keychain-encrypted and only the
    real Chrome binary (on the Keychain ACL) can decrypt them.
    """
    src = Path(args.chrome_source_profile).expanduser()
    if not (src / 'Default').exists():
        raise SystemExit(f'Chrome source profile not found (need a signed-in profile): {src}')
    if not Path(args.chrome_binary).exists():
        raise SystemExit(f'Chrome binary not found: {args.chrome_binary}')
    if not shutil.which('rsync'):
        raise SystemExit('rsync not found on PATH (needed for --cdp-profile)')

    tmpdir = Path(tempfile.mkdtemp(prefix='oa_cdp_chrome_'))
    (tmpdir / 'Default').mkdir(parents=True, exist_ok=True)
    shutil.copy2(src / 'Local State', tmpdir / 'Local State')
    rsync = ['rsync', '-a']
    for exclude in CDP_CACHE_EXCLUDES:
        rsync.extend(['--exclude', exclude])
    rsync.extend([str(src / 'Default') + '/', str(tmpdir / 'Default') + '/'])
    subprocess.run(rsync, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    chrome_cmd = [
        args.chrome_binary,
        f'--user-data-dir={tmpdir}',
        f'--remote-debugging-port={args.cdp_port}',
        '--remote-allow-origins=*',
        '--no-first-run', '--no-default-browser-check',
        '--window-size=1280,900',
        'about:blank',
    ]
    proc = subprocess.Popen(
        chrome_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            _cdp_http(args.cdp_port, '/json/version')
            break
        except (urllib.error.URLError, OSError, ConnectionError, json.JSONDecodeError):
            time.sleep(0.5)
    else:
        _kill_process_group(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise SystemExit(f'CDP did not come up on port {args.cdp_port} within 30s')

    print(f'[cdp] launched Chrome on profile copy {tmpdir} (port {args.cdp_port})', flush=True)
    return {'proc': proc, 'tmpdir': tmpdir, 'port': args.cdp_port}


def stop_cdp_chrome(state: dict[str, Any] | None) -> None:
    if not state:
        return
    proc = state.get('proc')
    if proc is not None:
        _kill_process_group(proc)
    shutil.rmtree(state.get('tmpdir'), ignore_errors=True)


def cdp_new_tab(port: int, url: str) -> str:
    """Open a fresh page tab and return its webSocketDebuggerUrl.

    Creating the tab explicitly (and connecting agent-browser to this specific
    target) avoids agent-browser's auto-pick grabbing the Gemini-in-Chrome
    companion target (gemini.google.com/glic) that this profile auto-opens.
    """
    target = url or 'about:blank'
    try:
        tab = _cdp_http(port, '/json/new?' + target, method='PUT', timeout=15)
    except urllib.error.HTTPError as exc:
        if exc.code != 405:
            raise
        tab = _cdp_http(port, '/json/new?' + target, method='GET', timeout=15)
    tid = tab.get('id')
    ws = tab.get('webSocketDebuggerUrl')
    if not (tid and ws):
        raise RuntimeError(f'CDP new tab missing id/webSocketDebuggerUrl: {tab}')
    # Wait for the tab to start navigating off about:blank, then make it the only
    # page target so agent-browser binds to it deterministically.
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        try:
            cur = next((t for t in _cdp_http(port, '/json') if t.get('id') == tid), None)
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            break
        if cur and (cur.get('url') or 'about:blank') != 'about:blank':
            break
        time.sleep(0.3)
    _cdp_close_other_pages(port, tid)
    return ws


def prepare_browser_session(row: dict[str, str], session: str, args: argparse.Namespace, log_file: Any) -> None:
    close_browser_session(args.repo, session)
    url = row.get('apply_url', '')
    if args.cdp_profile:
        # Open the role in a dedicated CDP tab that cdp_new_tab makes the only page
        # target, then bind agent-browser to it (see cdp_new_tab for why).
        ws = cdp_new_tab(args.cdp_port, url)
        run_agent_browser(
            args.repo,
            ['agent-browser', '--session', session, 'connect', ws],
            log_file,
            timeout=60,
        )
    else:
        browser_cmd = [
            'agent-browser',
            '--session', session,
        ]
        if args.browser_profile:
            browser_cmd.extend(['--profile', args.browser_profile])
        if args.browser_session_name:
            browser_cmd.extend(['--session-name', args.browser_session_name])
        if args.headed:
            browser_cmd.append('--headed')
        browser_cmd.extend([
            '--args', '--disable-blink-features=AutomationControlled',
            'open', url,
        ])
        run_agent_browser(
            args.repo,
            browser_cmd,
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
    lower = re.sub(r'^[\W_]+', '', text.lower())
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
        browser_instructions = f"""- A browser session is ALREADY prepared and the application URL is ALREADY open in it. On EVERY agent-browser command, pass exactly `--session {session}` (this exact flag and value) to target it.
- Do NOT pass `--session-name`; do NOT run `open`, `connect`, `navigate`, `launch`, or otherwise relaunch/reopen the browser before submitting. Those create a DIFFERENT browser context that loses the prepared signed-in session and gets the submission flagged as spam. Only inspect, fill, verify, and submit inside the existing `--session {session}`.
- Fill and verify the form manually using the applicant profile and policy before submitting."""
    else:
        browser_instructions = f"""- Use agent-browser for browser automation with unique session name: {session}.
- Open the application URL yourself.
- Verify all filled values before submitting."""

    is_ashby = row.get('id', '').startswith('ashby:') or row.get('source', '') == 'ashby'
    if is_ashby and not args.cdp_profile:
        ashby_note = """
Ashby note: submissions from this kind of session may be flagged as possible spam regardless of how the form is filled. Submit once, wait for the result, and if flagged report Blocked - do not retry in this session.
"""
    else:
        # In --cdp-profile mode the session carries the live signed-in identity, so
        # spam flags are not expected; submit once and read the result normally.
        ashby_note = ""

    return f"""You are an external job application agent. Apply to exactly one role.

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
{ashby_note}
Applicant data:
- Read applicant facts and commitments from {args.applicant_profile}. This JSON profile is the source of truth for contact details, resume path, work authorization, sponsorship, availability, preferences, clearances, screening facts, demographics preferences, education, and languages.
- Read reusable answer strategy from {args.application_policy}. Use it to apply profile facts to recurring form questions such as referral source, cover letters, in-office commitments, clearance, export-control status, compensation, transcript availability, and negative/conflict screening.
- Do not print or dump the full applicant profile or application policy into logs or the final message.
- If the profile and policy conflict on a factual answer, prefer the applicant profile. If a required answer is missing from both files, the resume, and visible application context, stop and return Needs input with the exact question text.

Rules:
- Do not apply to any other role.
- Do not invent facts.
- Keep every free-text / essay / open-ended / "why us" answer to 1-2 sentences maximum (see the answer policy's free_text_answer_length). Never write paragraph-length or multi-point narrative responses, even when a longer word limit is allowed.
- Submit only if all required answers are known.
- If a required answer cannot be inferred, stop and return Needs input with exact question text.
- On Greenhouse validation errors, inspect aria-invalid="true" / *-error fields. For invalid custom dropdowns, reselect via a different path; visible selected text alone may not clear validation.
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


def close_all_browser_sessions(repo: Path) -> None:
    """Stop the shared agent-browser daemon so launch flags are honored.

    agent-browser launch flags such as --headed and --args are daemon-scoped.
    If a headless daemon is already running, later per-session opens print a
    warning and ignore those flags. For application batches we prefer a clean
    daemon start so Ashby submits are not accidentally performed headless.
    """
    subprocess.run(
        ['agent-browser', 'close', '--all'],
        cwd=repo,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def build_codex_cmd(args: argparse.Namespace, prompt: str, final_path: Path) -> list[str]:
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
    return cmd


def build_claude_cmd(args: argparse.Namespace, prompt: str) -> list[str]:
    cmd = ['claude', '-p', '--dangerously-skip-permissions']
    if args.model:
        cmd.extend(['--model', args.model])
    cmd.append(prompt)
    return cmd


def _kill_process_group(proc: subprocess.Popen) -> None:
    """SIGKILL the subagent's whole process group, falling back to the lone child."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass


def run_subagent(cmd: list[str], *, cwd: Path, timeout: int, stdout: Any) -> tuple[int, str | None]:
    """Run a subagent, killing the whole process group on timeout.

    subprocess.run(timeout=) only kills the direct child, so codex/agent-browser
    grandchildren orphan and keep the stdout pipe open, hanging the worker thread
    far past --timeout-minutes (the timeout-leak that needed manual reaping).
    Launch in a new session so the subagent leads its own process group, then
    SIGKILL the entire group on timeout.

    Returns (return_code, captured_stdout). captured_stdout is the decoded stdout
    string when `stdout` is subprocess.PIPE (the claude backend, whose final
    message IS its stdout), and None when streaming straight to a file (codex).
    """
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
        return proc.returncode, out
    except subprocess.TimeoutExpired:
        _kill_process_group(proc)
        # group is dead, so any inherited pipe write-ends are closed: this drains
        # promptly instead of hanging the way the old subprocess.run(timeout=) did.
        try:
            out, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            out = None
        return 124, out


def run_agent(row: dict[str, str], index: int, args: argparse.Namespace, snapshot_date: str) -> dict[str, Any]:
    if RATE_LIMIT_EVENT.is_set():
        return {
            'id': row.get('id', ''),
            'status': 'skipped_rate_limit',
            'detail': 'Skipped: agent quota exhausted earlier in batch',
            'company_slug': row.get('company_slug', ''),
            'title': row.get('title', ''),
            'apply_url': row.get('apply_url', ''),
            'snapshot_date': snapshot_date,
            'agent': args.agent,
            '_skip_ledger': True,
        }

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

    if args.agent == 'codex':
        cmd = build_codex_cmd(args, prompt, final_path)
    else:
        cmd = build_claude_cmd(args, prompt)

    started_at = utc_now()
    return_code = None
    browser_setup_error: Exception | None = None
    try:
        with log_path.open('w', encoding='utf-8') as log_file:
            if args.prepare_browser:
                try:
                    prepare_browser_session(row, session, args, log_file)
                except (RuntimeError, OSError) as exc:
                    browser_setup_error = exc
                else:
                    log_file.write(f'\nPrepared browser session {session}\n')
                    log_file.flush()
            if browser_setup_error is None:
                if args.agent == 'claude':
                    # claude -p prints the final assistant message to stdout, so
                    # capture it: mirror it into the log and write it to final_path
                    # so the final-message extraction below treats it like codex's
                    # --output-last-message file.
                    return_code, captured = run_subagent(
                        cmd,
                        cwd=args.repo,
                        timeout=args.timeout_minutes * 60,
                        stdout=subprocess.PIPE,
                    )
                    if captured:
                        log_file.write(captured)
                        log_file.flush()
                        final_path.write_text(captured, encoding='utf-8')
                else:
                    return_code, _ = run_subagent(
                        cmd,
                        cwd=args.repo,
                        timeout=args.timeout_minutes * 60,
                        stdout=log_file,
                    )
    except subprocess.TimeoutExpired:
        return_code = 124
    finally:
        close_browser_session(args.repo, session)

    if browser_setup_error is not None:
        return {
            'id': row.get('id', ''),
            'status': 'infra_error',
            'detail': f'Browser setup failed: {browser_setup_error}',
            'company_slug': row.get('company_slug', ''),
            'title': row.get('title', ''),
            'apply_url': row.get('apply_url', ''),
            'snapshot_date': snapshot_date,
            'agent': args.agent,
            '_skip_ledger': True,
        }

    final_message = ''
    if final_path.exists():
        final_message = final_path.read_text(encoding='utf-8', errors='replace').strip()
    elif log_path.exists():
        data = log_path.read_text(encoding='utf-8', errors='replace')
        final_message = data[-4000:].strip()

    status, detail = parse_status(final_message)
    if return_code not in {0, None} and status == 'unknown':
        detail = f'{args.agent} exited with code {return_code}: {detail}'

    if is_rate_limited(final_message) or is_rate_limited(detail):
        if not RATE_LIMIT_EVENT.is_set():
            RATE_LIMIT_EVENT.set()
            print(f'[rate-limit] detected on role {index} ({row.get("company_slug","")}); short-circuiting remaining roles', flush=True)

    record = {
        'id': row.get('id', ''),
        'status': status,
        'detail': detail,
        'company_slug': row.get('company_slug', ''),
        'title': row.get('title', ''),
        'apply_url': row.get('apply_url', ''),
        'snapshot_date': snapshot_date,
        'session': session,
        'agent': args.agent,
        'agent_return_code': return_code,
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
    excluded_companies = {part.strip().lower() for part in args.exclude_company.split(',') if part.strip()}

    for row in rows:
        role_id = row.get('id', '')
        if only_ids and role_id not in only_ids:
            continue
        if excluded_companies and row.get('company_slug', '').lower() in excluded_companies:
            continue
        if not row.get('apply_url'):
            continue
        if not args.include_federal_defense and row.get('federal_defense_note') == 'check-federal/defense':
            continue
        previous = statuses.get(role_id)
        if previous:
            if previous.get('_ever_submitted'):
                continue  # never re-apply to anything we've ever successfully submitted
            previous_status = previous.get('status')
            if previous_status in SKIP_STATUSES and previous_status not in retry_statuses:
                continue
        selected.append(row)
        if args.limit and len(selected) >= args.limit:
            break
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply to latest shortlist roles with codex or claude subagents')
    parser.add_argument('--agent', choices=['claude', 'codex'], default='codex', help='which agent CLI to launch per role')
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
    parser.add_argument('--headless', dest='headed', action='store_false', help='Run agent-browser without --headed (no visible window). May trip anti-bot on stricter sites.')
    parser.add_argument('--headed', dest='headed', action='store_true', help='Run agent-browser with --headed (visible window, default).')
    parser.set_defaults(headed=True)
    parser.add_argument('--browser-profile', default='', help='agent-browser --profile value; default empty')
    parser.add_argument('--browser-session-name', default='openapply', help='agent-browser --session-name value')
    parser.add_argument('--browser-wait-ms', type=int, default=3000)
    parser.add_argument('--cdp-profile', action='store_true', help='EXPERIMENTAL: copy the real Chrome profile, launch it headed with a CDP port, and drive it via agent-browser CDP. Carries the live signed-in Google session Ashby reCAPTCHA/SEON scoring needs (6/6 in testing 2026-06-10); plain headless sessions are spam-flagged 50-86%% of the time. macOS only; needs a signed-in Chrome profile.')
    parser.add_argument('--cdp-port', type=int, default=9222, help='CDP port for --cdp-profile Chrome')
    parser.add_argument('--chrome-binary', default=DEFAULT_CHROME_BINARY, help='real Chrome binary for --cdp-profile')
    parser.add_argument('--chrome-source-profile', default=DEFAULT_CHROME_SOURCE_PROFILE, help='Chrome user-data dir to copy for --cdp-profile (must hold a signed-in profile)')
    parser.add_argument('--sandbox', default='danger-full-access', choices=['read-only', 'workspace-write', 'danger-full-access'])
    parser.add_argument('--model', default='')
    parser.add_argument('--reasoning-effort', default='low', help='codex model_reasoning_effort; higher modes are slower and can time out on routine form filling')
    parser.add_argument('--timeout-minutes', type=int, default=45)
    parser.add_argument('--retry-status', default='', help='comma-separated statuses to retry, e.g. needs_input,blocked')
    parser.add_argument('--only-id', default='', help='comma-separated role ids to apply regardless of order')
    parser.add_argument('--exclude-company', default='', help='comma-separated company_slug values to skip (e.g. openai for 180-day rate-limit window)')
    parser.add_argument('--include-federal-defense', action='store_true', help='allow auto-applying roles flagged as federal/defense instead of skipping them')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not args.model and args.agent == 'claude':
        args.model = 'sonnet'
    if not args.model and args.agent == 'codex':
        args.model = 'gpt-5.5'

    if args.cdp_profile and not args.prepare_browser:
        raise SystemExit('--cdp-profile requires --prepare-browser (the launcher connects each role to a CDP tab)')
    if args.cdp_profile and args.parallel > 1:
        print('[cdp] --cdp-profile drives one shared Chrome serially; forcing --parallel 1', flush=True)
        args.parallel = 1

    args.repo = args.repo.resolve()
    args.status = repo_path(args.repo, args.status)
    args.output_dir = repo_path(args.repo, args.output_dir.expanduser())
    args.applicant_profile = repo_path(args.repo, args.applicant_profile.expanduser())
    args.application_policy = repo_path(args.repo, args.application_policy.expanduser())
    args.skill_path = args.skill_path.expanduser()

    if not args.dry_run:
        if not shutil.which(args.agent):
            raise SystemExit(f'{args.agent} CLI not found on PATH')
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
    if args.prepare_browser and not args.cdp_profile:
        # Clear any stale agent-browser daemon sessions so daemon-scoped launch
        # flags (--headed/--args) are honored on a clean start. Skipped in
        # --cdp-profile mode: there we attach to the externally-launched Chrome via
        # `connect`, so the daemon's own browser and flags are irrelevant -- and
        # `close --all` would also tear down the unrelated hermes agent-browser
        # daemon that shares the default socket.
        close_all_browser_sessions(args.repo)
    cdp_state: dict[str, Any] | None = None
    if args.cdp_profile:
        cdp_state = start_cdp_chrome(args)
    print(f'Launching {len(todo)} role(s) from {latest["date"]} with parallel={max_workers}', flush=True)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for index, row in enumerate(todo, 1):
                print(f'Starting {index}/{len(todo)}: {row.get("company_slug", "")} - {row.get("title", "")}', flush=True)
                futures[executor.submit(run_agent, row, index, args, latest['date'])] = row
            for future in as_completed(futures):
                record = future.result()
                if record.pop('_skip_ledger', False):
                    reason = 'infra error' if record.get('status') == 'infra_error' else 'rate limit'
                    print(f'Skipped ({reason}, not ledgered): {record.get("company_slug","")} - {record.get("title","")} :: {record.get("detail","")}', flush=True)
                    continue
                append_status(args.status, record)
                print(f'Finished: {record["detail"]}', flush=True)
    finally:
        stop_cdp_chrome(cdp_state)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
