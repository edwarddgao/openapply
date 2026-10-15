#!/usr/bin/env python3
"""Refresh, shortlist, and apply unattended without creating a Codex app task."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, IO


REPO_ROOT = Path(__file__).resolve().parents[1]
# ~/.local/bin carries the `claude` CLI the application subagents run on.
DEFAULT_PATH = f'/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:{Path.home()}/.local/bin'
MIN_FREE_BYTES = 12 * 1024**3
# apply.py exits with this code (EX_TEMPFAIL) when the agent quota is exhausted
# mid-batch; unhandled roles remain, so wait for the reset and re-run.
QUOTA_EXIT_CODE = 75
# apply.py exits 77 (EX_NOPERM) when the claude CLI's login has failed; the
# phrases match its AUTH_FAILURE_PHRASES.
AUTH_FAILED_EXIT_CODE = 77
AUTH_FAILURE_PHRASES = ('failed to authenticate', 'oauth session expired', 'please run /login', 'invalid api key', 'not logged in')


def is_auth_failure(text: str) -> bool:
    low = (text or '').lower()
    return any(phrase in low for phrase in AUTH_FAILURE_PHRASES)


MAX_QUOTA_RESUMES = 3
QUOTA_WAIT_CAP_SECONDS = 6 * 3600
QUOTA_WAIT_FALLBACK_SECONDS = 3600
# claude -p quota errors state the reset clock time, e.g. "resets 3:20pm (America/Toronto)".
QUOTA_RESET_RE = re.compile(r'resets\s+(\d{1,2})(?::(\d{2}))?\s*([ap]m)', re.I)
# Recruiter aggregators that repost other companies' roles on their own ATS org.
# Every attempt gets blocked as recruiter-aggregator-funnel (jobgether: 6 blocks
# through 2026-08-19); excluding them saves agent quota for real employers.
AGGREGATOR_EXCLUDES = ('jobgether',)
# Tenant slug lists the weekly GitHub harvest (.github/workflows/harvest.yml)
# commits to origin/main. This checkout never pulled them, so the 2026-08-31
# refresh (+203 slugs) went unscraped locally for two weeks.
SLUG_FILES = tuple(f'cc_{ats}_FINAL.txt' for ats in ('greenhouse', 'lever', 'ashby', 'rippling', 'gem', 'workday', 'smartrecruiters'))
SLUG_REMOTE_REF = 'origin/main'
# Gmail reads go through IMAP with a keychain app password (scripts/gmail_imap.py).
# The old `gws` OAuth path handed out 7-day refresh tokens - the consent screen is
# already "In production", but it declares no scopes while gws requests restricted
# Gmail ones, so every grant was an unverified restricted-scope grant. App passwords
# do not expire, so there is no renewal clock left to warn about.
# Outcomes that are a normal end state and should not raise an alert.
QUIET_OUTCOMES = ('completed', 'dry_run_passed', 'refresh_only_completed', 'skipped_already_running')
CURRENT_CHILD: subprocess.Popen[str] | None = None
STOP_REQUESTED = False
APPLY_PROCESS_RE = re.compile(r'\bPython(?:\d+(?:\.\d+)*)?\s+\S*scripts/apply\.py(?:\s|$)', re.I)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def merge_slug_lines(local_text: str, remote_text: str) -> tuple[str, int]:
    """Union slug lists: keep every local line in order, append upstream-only ones."""
    seen: set[str] = set()
    merged: list[str] = []
    for line in local_text.splitlines():
        slug = line.strip()
        if slug and slug not in seen:
            seen.add(slug)
            merged.append(slug)
    added = 0
    for line in remote_text.splitlines():
        slug = line.strip()
        if slug and slug not in seen:
            seen.add(slug)
            merged.append(slug)
            added += 1
    return '\n'.join(merged) + '\n', added


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent, delete=False) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write('\n')
        temporary = Path(handle.name)
    temporary.replace(path)


def jsonl_tail(path: Path, first_line: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding='utf-8') as handle:
        for number, line in enumerate(handle, 1):
            if number < first_line or not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append({'status': 'malformed_ledger_record', 'detail': f'line {number}'})
    return rows


def line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open('rb') as handle:
        return sum(1 for _ in handle)


def parse_json_object(text: str) -> dict[str, Any]:
    """First JSON object in a command's output, ignoring anything after it.

    Subprocesses often print their JSON and then a human-readable error line on the
    same stream, so decoding to end-of-string raises "Extra data" exactly when the
    failure detail is needed.
    """
    start = text.find('{')
    if start < 0:
        raise ValueError('command did not return a JSON object')
    value, _ = json.JSONDecoder().raw_decode(text[start:])
    if not isinstance(value, dict):
        raise ValueError('command did not return a JSON object')
    return value


def has_apply_process(process_listing: str) -> bool:
    return any(APPLY_PROCESS_RE.search(line) for line in process_listing.splitlines())


def quota_wait_seconds(marker_path: Path) -> tuple[int, str]:
    """Seconds to wait before resuming after a quota wall, from apply.py's marker.

    Parses the reset clock time out of the persisted error message ("resets
    3:20pm (America/Toronto)" — local time on this machine). Falls back to a
    fixed wait when the marker is missing or unparseable, and caps the wait so a
    bad parse can never stall the runner into the next scheduled night.
    """
    detail = ''
    try:
        detail = json.loads(marker_path.read_text(encoding='utf-8')).get('detail', '')
    except (OSError, ValueError):
        pass
    match = QUOTA_RESET_RE.search(detail)
    if not match:
        return QUOTA_WAIT_FALLBACK_SECONDS, detail
    hour = int(match.group(1)) % 12 + (12 if match.group(3).lower() == 'pm' else 0)
    minute = int(match.group(2) or 0)
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    wait = int((target - now).total_seconds()) + 120  # small buffer past the reset
    return min(wait, QUOTA_WAIT_CAP_SECONDS), detail


def next_local_time(hhmm: str) -> datetime:
    """Next occurrence of a local HH:MM, fixed once per run so quota resumes share it."""
    hour, minute = (int(part) for part in hhmm.split(':'))
    now = datetime.now().astimezone()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target if target > now else target + timedelta(days=1)


def applescript_string(value: str) -> str:
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def gmail_check_command(repo: Path) -> list[str]:
    return [sys.executable, str(repo / 'scripts' / 'gmail_imap.py'), 'check']


def notify(title: str, message: str, repo: Path) -> None:
    """Surface a run problem outside the summary files.

    The 2026-08-25/26 auth failures went unnoticed for two days because the only
    record was summary.json, which nobody reads on a good morning. Alerts go to
    Notification Center and to an append-only log that survives a missed banner.
    """
    alerts = repo / 'applications' / 'overnight' / 'alerts.log'
    try:
        alerts.parent.mkdir(parents=True, exist_ok=True)
        with alerts.open('a', encoding='utf-8') as handle:
            handle.write(f'[{utc_now()}] {title}: {message}\n')
    except OSError:
        pass
    script = 'display notification {} with title {}'.format(
        applescript_string(message), applescript_string(title)
    )
    try:
        subprocess.run(
            ['/usr/bin/osascript', '-e', script],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def check_auth_only(repo: Path) -> int:
    """Standalone auth probe for a second launchd job scheduled before the pipeline.

    Running this in the evening turns an expired token into a banner you can act on
    that night, instead of a lost night discovered the next morning.
    """
    env = os.environ.copy()
    env['HOME'] = str(Path.home())
    env['PATH'] = DEFAULT_PATH
    try:
        proc = subprocess.run(
            gmail_check_command(repo), cwd=repo, env=env,
            capture_output=True, text=True, timeout=180,
        )
        parsed = parse_json_object(proc.stdout + proc.stderr)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        notify('openapply auth check', f'could not run the Gmail IMAP check: {exc}', repo)
        return 1
    if not parsed.get('available'):
        notify(
            'openapply auth check',
            f"Gmail IMAP unavailable ({parsed.get('error')}) - the next run will skip "
            'Greenhouse security-code roles.',
            repo,
        )
        return 1
    return 0


def stop_child(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    child = CURRENT_CHILD
    if child is None or child.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(child.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass


class OvernightRun:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.repo = args.repo.resolve()
        self.date = args.date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
        self.stop_at = next_local_time(args.stop_at) if args.stop_at else None
        run_stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        self.output_root = self.repo / 'applications' / 'overnight'
        self.run_dir = self.output_root / f'date={self.date}' / f'run={run_stamp}'
        self.log_path = self.run_dir / 'pipeline.log'
        self.summary_path = self.run_dir / 'summary.json'
        self.latest_summary = self.output_root / 'latest-summary.json'
        self.ledger = self.repo / 'applications' / 'status.jsonl'
        self.env = os.environ.copy()
        self.env['HOME'] = str(Path.home())
        self.env['PATH'] = DEFAULT_PATH
        self.last_return_code = 0
        self.summary: dict[str, Any] = {
            'date': self.date,
            'started_at': utc_now(),
            'finished_at': None,
            'outcome': 'running',
            'dry_run': args.dry_run,
            'no_apply': args.no_apply,
            'max_applications': args.max_applications,
            'stop_at': self.stop_at.isoformat() if self.stop_at else None,
            'stages': [],
            'shortlist': {},
            'applications': {},
            'gmail': {'checked': False, 'available': False, 'error': None},
            'error': None,
            'run_log': str(self.log_path.relative_to(self.repo)),
        }

    def save_summary(self) -> None:
        atomic_json(self.summary_path, self.summary)
        atomic_json(self.latest_summary, self.summary)

    def run_command(
        self,
        stage: str,
        command: list[str],
        log: IO[str],
        *,
        capture: bool = False,
        allowed_codes: tuple[int, ...] = (),
        optional: bool = False,
    ) -> str:
        global CURRENT_CHILD
        if STOP_REQUESTED:
            raise InterruptedError('overnight run stopped by operator')
        stage_record = {'name': stage, 'started_at': utc_now(), 'finished_at': None, 'status': 'running'}
        self.summary['stages'].append(stage_record)
        self.save_summary()
        log.write(f"\n[{stage_record['started_at']}] {stage}\n")
        log.write('$ ' + ' '.join(command) + '\n')
        log.flush()
        child = subprocess.Popen(
            command,
            cwd=self.repo,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE if capture else log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        CURRENT_CHILD = child
        output = ''
        try:
            captured, _ = child.communicate()
            output = captured or ''
            if output:
                log.write(output)
                log.flush()
        finally:
            CURRENT_CHILD = None
        if STOP_REQUESTED:
            raise InterruptedError('overnight run stopped by operator')
        stage_record['finished_at'] = utc_now()
        self.last_return_code = child.returncode
        if child.returncode == 0:
            stage_record['status'] = 'completed'
        elif child.returncode in allowed_codes:
            stage_record['status'] = 'quota_paused'
        elif optional:
            stage_record['status'] = 'degraded'
        else:
            stage_record['status'] = 'failed'
        stage_record['return_code'] = child.returncode
        self.save_summary()
        if child.returncode != 0 and child.returncode not in allowed_codes and not optional:
            raise RuntimeError(f'{stage} failed with exit code {child.returncode}')
        return output

    def preflight(self, log: IO[str]) -> None:
        required = ['claude', 'agent-browser', 'rsync']
        missing = [name for name in required if not shutil.which(name, path=self.env['PATH'])]
        if missing:
            raise RuntimeError('missing executables: ' + ', '.join(missing))
        chrome = Path('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome')
        if not chrome.exists():
            raise RuntimeError(f'Chrome not found: {chrome}')
        for relative in ('config/applicant-profile.json', 'config/application-policy.json'):
            path = self.repo / relative
            if not path.exists():
                raise RuntimeError(f'missing required configuration: {relative}')
            with path.open(encoding='utf-8') as handle:
                json.load(handle)
        if shutil.disk_usage(self.repo).free < MIN_FREE_BYTES:
            raise RuntimeError('less than 12 GiB free; refusing a multi-gigabyte refresh')
        ps = subprocess.run(
            ['/bin/ps', '-Ao', 'pid=,ppid=,command='], capture_output=True, text=True, check=True
        ).stdout
        if has_apply_process(ps):
            raise RuntimeError('another application pipeline is already running')
        self.check_agent_auth(log)
        self.check_gmail_auth(log)

    def check_agent_auth(self, log: IO[str]) -> None:
        """Fail before the refresh if the claude CLI's login has expired.

        On 2026-09-25 an expired OAuth session let the run spend 2 hours
        refreshing, then fail all 200 role agents one by one.
        """
        try:
            probe = subprocess.run(
                ['claude', '-p', '--strict-mcp-config', '--disable-slash-commands', '--tools=', 'Reply with just OK.'],
                capture_output=True, text=True, env=self.env, timeout=180, stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            log.write('agent auth probe: timed out; continuing (apply.py stops on a login failure)\n')
            return
        output = (probe.stdout + probe.stderr).strip()
        log.write(f'agent auth probe: exit {probe.returncode}: {output[:300]}\n')
        log.flush()
        if probe.returncode != 0 and is_auth_failure(output):
            raise RuntimeError(f'claude CLI is not logged in ({output[:200]}); run `claude /login`')

    def sync_slugs(self, log: IO[str]) -> None:
        """Merge tenant slugs from origin/main before the nightly scrape.

        The weekly harvest workflow only ever writes to origin/main. Slug files
        are unioned rather than checked out so a local-only slug is never lost,
        and a fetch failure only degrades the stage: last night's list is still
        a valid list, and any earlier fetch of the ref is merged regardless.
        """
        git = ['/usr/bin/git', '-C', str(self.repo)]
        self.run_command('fetch upstream slugs', git + ['fetch', '--quiet', 'origin', 'main'], log, optional=True)
        record: dict[str, Any] = {'ref': SLUG_REMOTE_REF, 'fetched': self.last_return_code == 0, 'added': {}}
        self.summary['slug_sync'] = record
        for name in SLUG_FILES:
            relative = f'slugs/{name}'
            shown = subprocess.run(
                git + ['show', f'{SLUG_REMOTE_REF}:{relative}'], capture_output=True, text=True, env=self.env
            )
            if shown.returncode != 0:
                log.write(f'slug sync: cannot read {SLUG_REMOTE_REF}:{relative}: {shown.stderr.strip()}\n')
                continue
            path = self.repo / relative
            local_text = path.read_text(encoding='utf-8') if path.exists() else ''
            merged, added = merge_slug_lines(local_text, shown.stdout)
            if added:
                path.write_text(merged, encoding='utf-8')
            record['added'][name] = added
            log.write(f'slug sync: {name} +{added}\n')
        log.flush()
        self.save_summary()

    def check_gmail_auth(self, log: IO[str]) -> None:
        """Record Gmail auth as run state instead of aborting on it.

        Gmail is only ever used to fetch Greenhouse security codes, which roughly 5%
        of Greenhouse roles ask for (169 blocked_on_gmail_auth against 3,368 Greenhouse
        submissions) and no Ashby or Lever role ever does. Treating it as a hard
        precondition threw away two entire nights on 2026-08-25/26 to protect a small
        slice of one ATS; the run now proceeds and skips only the code-gated roles.

        Access is an IMAP app password rather than an OAuth token, so an outage here
        means a missing or rejected keychain entry, not a routine weekly expiry.
        """
        gmail = self.summary['gmail']
        gmail['checked'] = True
        try:
            probe = self.run_command(
                'verify Gmail IMAP access', gmail_check_command(self.repo), log,
                capture=True, optional=True,
            )
            parsed = parse_json_object(probe)
            gmail['available'] = bool(parsed.get('available'))
            gmail['error'] = parsed.get('error')
        except (ValueError, json.JSONDecodeError) as exc:
            gmail['error'] = f'could not run the Gmail IMAP check: {exc}'
        if not gmail['available']:
            log.write(
                f"\nDEGRADED: Gmail IMAP unavailable ({gmail['error']}). Continuing without it;"
                ' roles needing an emailed verification code will be recorded as'
                ' blocked_on_gmail_auth and stay retryable.\n'
            )
        log.flush()
        self.save_summary()

    def wait_for_quota_reset(self, seconds: int, detail: str, log: IO[str], attempt: int) -> None:
        stage_record = {
            'name': f'wait for quota reset (resume {attempt})',
            'started_at': utc_now(),
            'finished_at': None,
            'status': 'running',
            'wait_seconds': seconds,
        }
        self.summary['stages'].append(stage_record)
        self.save_summary()
        log.write(f'\n[{stage_record["started_at"]}] waiting {seconds}s for agent quota reset'
                  f' :: {detail[:160]}\n')
        log.flush()
        remaining = seconds
        while remaining > 0:
            if STOP_REQUESTED:
                raise InterruptedError('overnight run stopped by operator')
            step = min(30, remaining)
            time.sleep(step)
            remaining -= step
        stage_record['finished_at'] = utc_now()
        stage_record['status'] = 'completed'
        self.save_summary()

    def emit_alerts(self) -> None:
        outcome = self.summary.get('outcome')
        gmail = self.summary.get('gmail') or {}
        applications = self.summary.get('applications') or {}
        problems: list[str] = []
        if outcome not in QUIET_OUTCOMES:
            problems.append(f"run {outcome}: {self.summary.get('error') or 'see pipeline.log'}")
        if gmail.get('checked') and not gmail.get('available'):
            problems.append(f"Gmail IMAP unavailable ({gmail.get('error')}), "
                            'security-code roles skipped')
        # A run that finishes cleanly but submits nothing looks identical to a good
        # night in the summary. Volume collapsed 67 -> 4 -> 7 over 2026-08-22..24 on
        # runs that all reported 'completed', so make an empty night say so.
        if outcome == 'completed' and not applications.get('attempted'):
            problems.append('run completed but submitted nothing '
                            f"({applications.get('eligible', 0)} eligible)")
        if problems:
            notify(f'openapply {self.date}', ' | '.join(problems), self.repo)

    def application_command(self, *, dry_run: bool, limit: int | None = None) -> list[str]:
        command = [
            sys.executable,
            str(self.repo / 'scripts' / 'apply.py'),
            '--agent', 'claude',
            '--model', self.args.model,
            '--repo', str(self.repo),
            '--latest', 'shortlists/latest.json',
            # Both pools, not just fresh: the fresh pool saturates (every role in it is
            # already in the ledger by 2026-08-24), so 'fresh' alone left 190 eligible
            # backlog roles permanently unapplied. The ledger keeps re-runs idempotent.
            '--pool', 'both',
            '--limit', str(self.args.max_applications if limit is None else limit),
            '--exclude-company', ','.join(AGGREGATOR_EXCLUDES),
            '--parallel', str(self.args.parallel),
            '--cdp-profile',
            '--headed',
        ]
        if self.args.effort:
            command.extend(['--effort', self.args.effort])
        if not self.summary['gmail']['available']:
            command.append('--no-gmail')
        # Recover yesterday's transient Ashby spam flags (87% submit on a later-day retry).
        command.append('--retry-spam')
        command.extend(['--source-cap', self.args.source_cap])
        if self.stop_at:
            command.extend(['--stop-at', self.stop_at.isoformat()])
        if dry_run:
            command.append('--dry-run')
        return command

    def execute(self) -> int:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.output_root / 'run.lock'
        caffeinate = subprocess.Popen(
            # -s also blocks system sleep while on AC power: quota-reset waits can
            # last hours, and the 2026-08-18 run lost 7h to maintenance/clamshell
            # sleep that -i alone does not prevent. (-s is inert on battery.)
            ['/usr/bin/caffeinate', '-ims', '-w', str(os.getpid())],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with lock_path.open('a+') as lock, self.log_path.open('a', encoding='utf-8') as log:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.summary['outcome'] = 'skipped_already_running'
                self.summary['finished_at'] = utc_now()
                self.save_summary()
                caffeinate.terminate()
                return 0
            try:
                self.preflight(log)
                if self.args.dry_run:
                    preview = self.run_command(
                        'preview current shortlist', self.application_command(dry_run=True), log, capture=True
                    )
                    self.summary['applications'] = {
                        'eligible': len(parse_json_object(preview).get('selected', [])),
                        'attempted': 0,
                    }
                    self.summary['outcome'] = 'dry_run_passed'
                    return 0

                self.sync_slugs(log)
                self.run_command(
                    'refresh public ATS data',
                    [sys.executable, str(self.repo / 'oa_adapter.py'), '--workers', str(self.args.workers), '--out', 'jobs.jsonl'],
                    log,
                )
                self.run_command(
                    'convert daily parquet snapshot',
                    [sys.executable, str(self.repo / 'scripts' / 'jsonl_to_parquet.py'), 'jobs.jsonl', 'data', '--date', self.date],
                    log,
                )
                self.run_command(
                    'build local shortlist',
                    # --jev defaults to shadow mode: it only annotates rows (see scripts/jev_report.py).
                    [sys.executable, str(self.repo / 'scripts' / 'build_shortlist.py'), '--data-dir', 'data', '--date', self.date, '--out-dir', 'shortlists', '--jev'],
                    log,
                )
                shortlist_path = self.repo / 'shortlists' / f'date={self.date}' / 'summary.json'
                with shortlist_path.open(encoding='ascii') as handle:
                    shortlist = json.load(handle)
                self.summary['shortlist'] = {
                    key: shortlist.get(key)
                    for key in ('date', 'source_rows', 'fresh_rows', 'backlog_rows', 'removed_rows', 'jev')
                }
                preview = self.run_command(
                    'select unhandled applications', self.application_command(dry_run=True), log, capture=True
                )
                eligible = len(parse_json_object(preview).get('selected', []))
                if self.args.no_apply:
                    self.summary['applications'] = {'eligible': eligible, 'attempted': 0}
                    self.summary['outcome'] = 'refresh_only_completed'
                    return 0
                ledger_start = line_count(self.ledger) + 1
                # A quota wall mid-batch exits QUOTA_EXIT_CODE with unhandled roles
                # remaining. Wait for the stated reset, then re-run; the ledger makes
                # re-runs idempotent. Bounded so a persistent wall (e.g. an exhausted
                # weekly cap) cannot stall into the next scheduled night.
                resumes = 0
                self.run_command(
                    'apply to selected roles',
                    self.application_command(dry_run=False),
                    log,
                    allowed_codes=(QUOTA_EXIT_CODE, AUTH_FAILED_EXIT_CODE),
                )
                if self.last_return_code == AUTH_FAILED_EXIT_CODE:
                    raise RuntimeError('claude CLI login failed mid-run; run `claude /login` (unapplied roles stay eligible)')
                while self.last_return_code == QUOTA_EXIT_CODE and resumes < MAX_QUOTA_RESUMES:
                    # The cap is per night, not per apply.py launch: 2026-09-24 attempted
                    # 253 against 200 because each resume got the full --limit again.
                    remaining = self.args.max_applications - len(jsonl_tail(self.ledger, ledger_start))
                    if remaining < 1:
                        break
                    wait, detail = quota_wait_seconds(self.repo / 'applications' / 'rate_limit_marker.json')
                    if self.stop_at and datetime.now().astimezone() + timedelta(seconds=wait) >= self.stop_at:
                        break
                    resumes += 1
                    self.wait_for_quota_reset(wait, detail, log, resumes)
                    self.run_command(
                        f'apply to selected roles (quota resume {resumes})',
                        self.application_command(dry_run=False, limit=remaining),
                        log,
                        allowed_codes=(QUOTA_EXIT_CODE,),
                    )
                records = jsonl_tail(self.ledger, ledger_start)
                counts = Counter(str(row.get('status', 'unknown')) for row in records)
                unresolved = [
                    {
                        'id': row.get('id'),
                        'company_slug': row.get('company_slug'),
                        'title': row.get('title'),
                        'status': row.get('status'),
                        'detail': row.get('detail'),
                    }
                    for row in records if row.get('status') != 'submitted'
                ]
                unresolved_path = self.run_dir / 'unresolved.json'
                atomic_json(unresolved_path, {'date': self.date, 'roles': unresolved})
                self.summary['applications'] = {
                    'eligible': eligible,
                    'attempted': len(records),
                    'status_counts': dict(sorted(counts.items())),
                    'unresolved_count': len(unresolved),
                    'unresolved_path': str(unresolved_path.relative_to(self.repo)),
                    'quota_resumes': resumes,
                    'quota_exhausted': self.last_return_code == QUOTA_EXIT_CODE,
                }
                self.summary['outcome'] = 'completed'
                return 0
            except Exception as exc:
                self.summary['outcome'] = 'failed'
                self.summary['error'] = str(exc)
                log.write(f'\nFAILED: {exc}\n')
                log.flush()
                return 1
            finally:
                self.summary['finished_at'] = utc_now()
                self.save_summary()
                self.emit_alerts()
                caffeinate.terminate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=REPO_ROOT)
    parser.add_argument('--date', default='', help='UTC snapshot date; default today')
    parser.add_argument('--workers', type=int, default=32, help='ATS refresh workers')
    # The stop time, not the count, ends a normal night: from a 03:00 start the apply
    # stage runs ~04:00-09:00, and four workers manage ~2 roles/min outside Workday.
    parser.add_argument('--max-applications', type=int, default=500, help='daily application safety cap')
    parser.add_argument('--stop-at', default='09:00',
                        help='local HH:MM after which no new role starts (next occurrence); empty disables')
    # No Workday cap: --stop-at bounds the night and Workday (~9 min/role) sorts last,
    # so it only fills time the faster ATSs leave over.
    parser.add_argument('--source-cap', default='workable=50',
                        help='per-run caps for slow ATSs, name=N; an unlisted ATS is uncapped')
    # Serial runs took 6-9 h of applying (2026-09-27..10-09), finishing 12:00-16:30
    # from a 05:00 start. Each worker drives its own profile-backed Chrome.
    parser.add_argument('--parallel', type=int, default=4, help='roles applied concurrently, one Chrome each')
    parser.add_argument('--model', default='sonnet', help='claude model for the role agents')
    parser.add_argument('--effort', default='', help="claude --effort for the role agents; empty keeps Claude Code's default")
    parser.add_argument('--dry-run', action='store_true', help='validate dependencies and preview without refresh or submission')
    parser.add_argument('--no-apply', action='store_true', help='refresh and build the shortlist without submitting applications')
    parser.add_argument('--check-auth-only', action='store_true',
                        help='probe Gmail auth, alert if it is invalid or near the 7-day '
                             'testing-mode expiry, and exit without running the pipeline')
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('--workers must be positive')
    if args.max_applications < 1:
        parser.error('--max-applications must be positive')
    return args


def main() -> int:
    signal.signal(signal.SIGTERM, stop_child)
    signal.signal(signal.SIGINT, stop_child)
    args = parse_args()
    if args.check_auth_only:
        return check_auth_only(args.repo.resolve())
    return OvernightRun(args).execute()


if __name__ == '__main__':
    raise SystemExit(main())
