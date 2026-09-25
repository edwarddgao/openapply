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
MAX_QUOTA_RESUMES = 3
QUOTA_WAIT_CAP_SECONDS = 6 * 3600
QUOTA_WAIT_FALLBACK_SECONDS = 3600
# claude -p quota errors state the reset clock time, e.g. "resets 3:20pm (America/Toronto)".
QUOTA_RESET_RE = re.compile(r'resets\s+(\d{1,2})(?::(\d{2}))?\s*([ap]m)', re.I)
# Recruiter aggregators that repost other companies' roles on their own ATS org.
# Every attempt gets blocked as recruiter-aggregator-funnel (jobgether: 6 blocks
# through 2026-08-19); excluding them saves agent quota for real employers.
AGGREGATOR_EXCLUDES = ('jobgether',)
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
        self.check_gmail_auth(log)

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

    def application_command(self, *, dry_run: bool) -> list[str]:
        command = [
            sys.executable,
            str(self.repo / 'scripts' / 'apply.py'),
            '--agent', 'claude',
            '--model', 'sonnet',
            '--repo', str(self.repo),
            '--latest', 'shortlists/latest.json',
            # Both pools, not just fresh: the fresh pool saturates (every role in it is
            # already in the ledger by 2026-08-24), so 'fresh' alone left 190 eligible
            # backlog roles permanently unapplied. The ledger keeps re-runs idempotent.
            '--pool', 'both',
            '--limit', str(self.args.max_applications),
            '--exclude-company', ','.join(AGGREGATOR_EXCLUDES),
            '--parallel', '1',
            '--cdp-profile',
            '--headed',
        ]
        if not self.summary['gmail']['available']:
            command.append('--no-gmail')
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
                    [sys.executable, str(self.repo / 'scripts' / 'build_shortlist.py'), '--data-dir', 'data', '--date', self.date, '--out-dir', 'shortlists'],
                    log,
                )
                shortlist_path = self.repo / 'shortlists' / f'date={self.date}' / 'summary.json'
                with shortlist_path.open(encoding='ascii') as handle:
                    shortlist = json.load(handle)
                self.summary['shortlist'] = {
                    key: shortlist.get(key)
                    for key in ('date', 'source_rows', 'fresh_rows', 'backlog_rows', 'removed_rows')
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
                    allowed_codes=(QUOTA_EXIT_CODE,),
                )
                while self.last_return_code == QUOTA_EXIT_CODE and resumes < MAX_QUOTA_RESUMES:
                    resumes += 1
                    wait, detail = quota_wait_seconds(self.repo / 'applications' / 'rate_limit_marker.json')
                    self.wait_for_quota_reset(wait, detail, log, resumes)
                    self.run_command(
                        f'apply to selected roles (quota resume {resumes})',
                        self.application_command(dry_run=False),
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
    parser.add_argument('--max-applications', type=int, default=200, help='daily application safety cap')
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
