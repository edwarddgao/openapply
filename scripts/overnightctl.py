#!/usr/bin/env python3
"""Install and operate the recurring OpenApply macOS LaunchAgent."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
LABEL = 'com.openapply.overnight'
DOMAIN = f'gui/{os.getuid()}'
SERVICE = f'{DOMAIN}/{LABEL}'
PLIST_PATH = Path.home() / 'Library' / 'LaunchAgents' / f'{LABEL}.plist'
# ~/.local/bin carries the `claude` CLI the application subagents run on.
DEFAULT_PATH = f'/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:{Path.home()}/.local/bin'


def call(command: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, capture_output=capture, text=True)


def service_loaded() -> bool:
    return call(['launchctl', 'print', SERVICE], check=False, capture=True).returncode == 0


def plist(repo: Path, hour: int, minute: int, max_applications: int) -> dict[str, Any]:
    python = repo / '.venv' / 'bin' / 'python'
    if not python.exists():
        raise SystemExit(f'virtualenv Python not found: {python}')
    output = repo / 'applications' / 'overnight'
    return {
        'Label': LABEL,
        'ProgramArguments': [
            str(python), str(repo / 'scripts' / 'run_overnight.py'),
            '--repo', str(repo),
            '--max-applications', str(max_applications),
        ],
        'WorkingDirectory': str(repo),
        'EnvironmentVariables': {'HOME': str(Path.home()), 'PATH': DEFAULT_PATH},
        'StartCalendarInterval': {'Hour': hour, 'Minute': minute},
        'RunAtLoad': False,
        'ProcessType': 'Background',
        'LowPriorityIO': True,
        'ThrottleInterval': 60,
        'ExitTimeOut': 60,
        'StandardOutPath': str(output / 'launchd.out.log'),
        'StandardErrorPath': str(output / 'launchd.err.log'),
    }


def install(args: argparse.Namespace) -> None:
    repo = args.repo.resolve()
    (repo / 'applications' / 'overnight').mkdir(parents=True, exist_ok=True)
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = plist(repo, args.hour, args.minute, args.max_applications)
    with tempfile.NamedTemporaryFile('wb', dir=PLIST_PATH.parent, delete=False) as handle:
        plistlib.dump(payload, handle, sort_keys=True)
        temporary = Path(handle.name)
    temporary.chmod(0o644)
    temporary.replace(PLIST_PATH)
    call(['plutil', '-lint', str(PLIST_PATH)])
    if service_loaded():
        call(['launchctl', 'bootout', SERVICE], check=False)
    call(['launchctl', 'enable', SERVICE])
    call(['launchctl', 'bootstrap', DOMAIN, str(PLIST_PATH)])
    print(f'installed {LABEL} for {args.hour:02d}:{args.minute:02d} local time')


def pause(_args: argparse.Namespace) -> None:
    if service_loaded():
        call(['launchctl', 'bootout', SERVICE], check=False)
    call(['launchctl', 'disable', SERVICE])
    print('overnight automation paused')


def resume(_args: argparse.Namespace) -> None:
    if not PLIST_PATH.exists():
        raise SystemExit(f'not installed: {PLIST_PATH}')
    call(['launchctl', 'enable', SERVICE])
    if not service_loaded():
        call(['launchctl', 'bootstrap', DOMAIN, str(PLIST_PATH)])
    print('overnight automation resumed')


def stop(_args: argparse.Namespace) -> None:
    if not service_loaded():
        print('service is paused or not installed')
        return
    state = call(['launchctl', 'print', SERVICE], capture=True).stdout
    if 'state = running' not in state:
        print('service is scheduled but not currently running')
        return
    call(['launchctl', 'kill', 'SIGTERM', SERVICE], check=False)
    print('stop requested; future nightly runs remain scheduled')


def start(_args: argparse.Namespace) -> None:
    if not service_loaded():
        raise SystemExit('service is paused or not installed')
    call(['launchctl', 'kickstart', SERVICE])
    print('manual run started')


def status(args: argparse.Namespace) -> None:
    result = call(['launchctl', 'print', SERVICE], check=False, capture=True)
    print(result.stdout.strip() if result.returncode == 0 else 'service is not loaded')
    latest = args.repo.resolve() / 'applications' / 'overnight' / 'latest-summary.json'
    if latest.exists():
        print('\nlatest summary:')
        print(json.dumps(json.loads(latest.read_text(encoding='utf-8')), indent=2, sort_keys=True))


def uninstall(_args: argparse.Namespace) -> None:
    if service_loaded():
        call(['launchctl', 'bootout', SERVICE], check=False)
    call(['launchctl', 'disable', SERVICE], check=False)
    PLIST_PATH.unlink(missing_ok=True)
    print('overnight automation uninstalled; application data and logs were preserved')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['install', 'status', 'start', 'stop', 'pause', 'resume', 'uninstall'])
    parser.add_argument('--repo', type=Path, default=REPO_ROOT)
    parser.add_argument('--hour', type=int, default=3)
    parser.add_argument('--minute', type=int, default=0)
    parser.add_argument('--max-applications', type=int, default=500)
    args = parser.parse_args()
    if not 0 <= args.hour <= 23 or not 0 <= args.minute <= 59:
        parser.error('invalid schedule time')
    if args.max_applications < 1:
        parser.error('--max-applications must be positive')
    return args


def main() -> None:
    args = parse_args()
    globals()[args.command](args)


if __name__ == '__main__':
    main()
