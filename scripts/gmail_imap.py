#!/usr/bin/env python3
"""Fetch Greenhouse security codes over IMAP with a non-expiring app password.

Replaces the `gws` OAuth path. The OAuth client for this project requests
restricted Gmail scopes against a consent screen that declares none, so Google
issues 7-day refresh tokens no matter the publishing status (already "In
production"). That cost two entire nights on 2026-08-25/26 and a re-login every
week before that. Gmail app passwords do not expire, so this ends the treadmill.

The password lives in the login keychain, never in the repo:

    security add-generic-password -s openapply-gmail-imap \\
        -a edwarddgao@gmail.com -w

(-w with no value prompts, so the secret never reaches a shell history or a log.)
"""

from __future__ import annotations

import argparse
import email
import imaplib
import json
import quopri
import re
import subprocess
import sys
import time
from email.header import decode_header, make_header
from email.message import Message
from html import unescape
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
KEYCHAIN_SERVICE = 'openapply-gmail-imap'
ENV_FALLBACK = 'OPENAPPLY_GMAIL_APP_PASSWORD'
IMAP_HOST = 'imap.gmail.com'
IMAP_PORT = 993
# All Mail rather than INBOX: a filter or an archive sweep must not hide a code.
MAILBOX = '"[Gmail]/All Mail"'
GREENHOUSE_SENDERS = ('no-reply@us.greenhouse-mail.io', 'no-reply@eu.greenhouse-mail.io')
CODE_SUBJECT = 'Security code for your application'
# Greenhouse security codes are 8 mixed-case alphanumerics (N9NYu5Bu, UOrA7rR7,
# xQHmGBMT), filling fields #security-input-0..7. Case matters: an [A-Z0-9] pattern
# silently misses most real codes.
CODE_ANCHOR_RE = re.compile(
    r'security code field on your application:\s*([A-Za-z0-9]{8})\b', re.I
)
CODE_TOKEN_RE = re.compile(r'\b([A-Za-z0-9]{8})\b')

AUTH_EXIT = 2


class AuthError(RuntimeError):
    """Credentials are missing or rejected: every role in the batch will fail."""


def app_password(address: str) -> str:
    try:
        found = subprocess.run(
            ['/usr/bin/security', 'find-generic-password',
             '-s', KEYCHAIN_SERVICE, '-a', address, '-w'],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthError(f'keychain lookup failed: {exc}') from exc
    if found.returncode == 0 and found.stdout.strip():
        return found.stdout.strip()
    import os
    fallback = os.environ.get(ENV_FALLBACK, '').strip()
    if fallback:
        return fallback
    raise AuthError(
        f'no app password for {address}. Add one with: '
        f'security add-generic-password -s {KEYCHAIN_SERVICE} -a {address} -w'
    )


def profile_address() -> str:
    path = REPO_ROOT / 'config' / 'applicant-profile.json'
    with path.open(encoding='utf-8') as handle:
        profile = json.load(handle)
    for section in profile.values():
        if isinstance(section, dict) and section.get('email'):
            return str(section['email'])
    raise RuntimeError(f'no contact email in {path}')


def connect(address: str) -> imaplib.IMAP4_SSL:
    try:
        client = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    except OSError as exc:
        raise RuntimeError(f'cannot reach {IMAP_HOST}: {exc}') from exc
    try:
        client.login(address, app_password(address))
    except imaplib.IMAP4.error as exc:
        # Gmail answers a bad app password and a disabled-IMAP account the same way.
        raise AuthError(f'IMAP login rejected for {address}: {exc}') from exc
    return client


def decoded_text(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        return ''
    charset = part.get_content_charset() or 'utf-8'
    try:
        return payload.decode(charset, errors='replace')
    except LookupError:
        return payload.decode('utf-8', errors='replace')


def message_text(message: Message) -> str:
    """Plain text preferred; HTML stripped as a fallback so codes in styled mail land."""
    plain: list[str] = []
    html: list[str] = []
    for part in message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get_content_type() == 'text/plain':
            plain.append(decoded_text(part))
        elif part.get_content_type() == 'text/html':
            html.append(decoded_text(part))
    if plain:
        return '\n'.join(plain)
    stripped = re.sub(r'<[^>]+>', ' ', '\n'.join(html))
    return unescape(quopri.decodestring(stripped.encode()).decode('utf-8', 'replace'))


def code_shaped(token: str) -> bool:
    """Reject ordinary 8-letter words without rejecting real codes.

    Greenhouse codes carry a digit or several capitals; English words in the mail
    body ("Greenhou", "resubmit") carry at most a single leading capital.
    """
    if any(char.isdigit() for char in token):
        return True
    return sum(1 for char in token if char.isupper()) >= 2


def extract_code(subject: str, body: str) -> tuple[str | None, str]:
    """Return (code, surrounding text) so the caller can eyeball the match."""
    text = subject + '\n' + body
    anchored = CODE_ANCHOR_RE.search(text)
    if anchored:
        start = max(0, anchored.start() - 40)
        return anchored.group(1), ' '.join(text[start:anchored.end() + 40].split())
    # The wording has been stable for 201 messages, but do not let a reworded
    # template turn into a silent "no code found" across a whole night.
    for line in text.splitlines():
        for candidate in CODE_TOKEN_RE.findall(line):
            if code_shaped(candidate):
                return candidate, ' '.join(line.split())[:200]
    return None, ''


def company_matches(subject: str, company: str) -> bool:
    """Loose subject match: 'Veterinary Emergency Group (VEG)' vs a shortlist slug.

    Compares on alphanumerics only, so punctuation, case, and spacing differences
    between the shortlist name and the mail subject do not cause a miss.
    """
    if not company:
        return True
    normalize = lambda value: re.sub(r'[^a-z0-9]+', '', value.lower())
    return normalize(company) in normalize(subject)


def search_codes(client: imaplib.IMAP4_SSL, company: str, within_minutes: int) -> list[dict[str, Any]]:
    """Search recent Greenhouse code mail, then filter by company locally.

    X-GM-RAW rejects a query containing double quotes ("BAD Could not parse
    command"), and company names carry parens and punctuation that break the
    grammar too ("Veterinary Emergency Group (VEG)"). So the server query stays
    quote-free and structural, and the company match happens here.
    """
    senders = ' OR '.join(f'from:{sender}' for sender in GREENHOUSE_SENDERS)
    days = max(1, (within_minutes + 1439) // 1440)
    query = f'({senders}) subject:({CODE_SUBJECT}) newer_than:{days}d'
    client.select(MAILBOX, readonly=True)
    status, data = client.search(None, 'X-GM-RAW', f'"{query}"')
    if status != 'OK' or not data or not data[0]:
        return []
    results: list[dict[str, Any]] = []
    cutoff = time.time() - within_minutes * 60
    for uid in data[0].split()[-25:]:
        status, fetched = client.fetch(uid, '(RFC822)')
        if status != 'OK' or not fetched or not isinstance(fetched[0], tuple):
            continue
        message = email.message_from_bytes(fetched[0][1])
        subject = str(make_header(decode_header(message.get('Subject', ''))))
        if not company_matches(subject, company):
            continue
        received = email.utils.parsedate_to_datetime(message.get('Date', ''))
        if received and received.timestamp() < cutoff:
            continue
        body = message_text(message)
        code, context = extract_code(subject, body)
        if code:
            results.append({
                'code': code,
                'subject': subject,
                'received': received.isoformat() if received else None,
                'context': context,
            })
    results.sort(key=lambda row: row['received'] or '', reverse=True)
    return results


def cmd_security_code(args: argparse.Namespace) -> int:
    address = args.address or profile_address()
    client = connect(address)
    try:
        deadline = time.time() + args.wait
        while True:
            matches = search_codes(client, args.company, args.within_minutes)
            if matches:
                print(json.dumps(matches[0], indent=2))
                return 0
            if time.time() >= deadline:
                break
            time.sleep(args.poll_interval)
    finally:
        try:
            client.logout()
        except (imaplib.IMAP4.error, OSError):
            pass
    print(json.dumps({
        'error': 'no security code found',
        'company': args.company,
        'waited_seconds': args.wait,
    }, indent=2))
    return 1


def cmd_check(args: argparse.Namespace) -> int:
    address = args.address or profile_address()
    result: dict[str, Any] = {'address': address, 'available': False, 'error': None}
    try:
        client = connect(address)
        client.select(MAILBOX, readonly=True)
        client.logout()
        result['available'] = True
    except (AuthError, RuntimeError, imaplib.IMAP4.error, OSError) as exc:
        result['error'] = str(exc)
    print(json.dumps(result, indent=2))
    return 0 if result['available'] else AUTH_EXIT


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--address', default='', help='mailbox; default: applicant-profile email')
    sub = parser.add_subparsers(dest='command', required=True)

    check = sub.add_parser('check', help='verify IMAP credentials work')
    check.set_defaults(func=cmd_check)

    code = sub.add_parser('security-code', help='fetch a Greenhouse security code')
    code.add_argument('--company', default='', help='company name as it appears in the subject')
    code.add_argument('--wait', type=int, default=90, help='seconds to keep polling (default 90)')
    code.add_argument('--poll-interval', type=int, default=5)
    code.add_argument('--within-minutes', type=int, default=15,
                      help='ignore codes older than this (default 15)')
    code.set_defaults(func=cmd_security_code)

    args = parser.parse_args()
    try:
        return args.func(args)
    except AuthError as exc:
        print(json.dumps({'error': str(exc), 'auth': False}, indent=2), file=sys.stderr)
        return AUTH_EXIT
    except RuntimeError as exc:
        print(json.dumps({'error': str(exc)}, indent=2), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
