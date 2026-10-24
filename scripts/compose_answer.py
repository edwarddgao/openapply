#!/usr/bin/env python3
"""Draft a truthful free-text application answer with Opus.

The overnight role agents run on Haiku 5.5 (cheap, fast at form mechanics). Written
answers - "describe a project", "why this company", "tell us about..." - are the one
place a stronger writer pays for itself, so the agent delegates them here:

    python3 scripts/compose_answer.py --company fluidstack --title "Product Engineer" \
        --question "What interests you about working at Fluidstack?"

Prints the answer text alone. If the profile and resume hold no truthful basis for an
answer, prints `NO_TRUTHFUL_ANSWER: <reason>` and exits 2 so the agent falls back to
its Blocked / Needs input rules instead of guessing. Never invents experience.

Uses the `claude` CLI (same login as the role agents; no API key needed). One call
costs a few cents on Opus 5.5: the profile, policy and resume text are ~4k tokens.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = Path(os.environ.get('OPENAPPLY_APPLICANT_PROFILE', REPO / 'config/applicant-profile.json'))
DEFAULT_POLICY = Path(os.environ.get('OPENAPPLY_APPLICATION_POLICY', REPO / 'config/application-policy.json'))
DEFAULT_MODEL = 'claude-opus-5-5'
NO_ANSWER = 'NO_TRUTHFUL_ANSWER'


def resume_text(pdf: Path) -> str:
    if not pdf.exists():
        return ''
    if shutil.which('pdftotext'):
        out = subprocess.run(['pdftotext', '-layout', str(pdf), '-'], capture_output=True, text=True)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout
    try:
        import pypdf  # type: ignore
        return '\n'.join(page.extract_text() or '' for page in pypdf.PdfReader(str(pdf)).pages)
    except Exception:  # noqa: BLE001 - any extraction failure just means no resume text
        return ''


def build_prompt(args: argparse.Namespace, profile: dict, policy: dict, resume: str) -> str:
    role = ' - '.join(part for part in (args.company, args.title) if part)
    context = f'\nAdditional context from the form or posting:\n{args.context}\n' if args.context else ''
    return f"""You are drafting one answer for a job application form on behalf of the applicant described below. The applicant will paste your text into the form verbatim and submit it under their own name.

Role: {role or '(not given)'}
Form question (answer this and nothing else):
{args.question}
{context}
Rules:
- Use only facts that appear in the applicant profile, policy, or resume below. Do not invent employers, projects, metrics, tools, dates, or experience. Do not embellish.
- If the question requires experience, facts, or opinions that these materials do not support (for example a technology or domain the resume never mentions, or a specific past situation that is not described), reply with exactly one line: `{NO_ANSWER}: <one short reason>` and nothing else.
- Write in the first person, plain and specific, as the applicant would. No greetings, sign-offs, headings, bullet points, quotes, or markdown. No filler such as "I am passionate" or "I am excited". Mention the company only if the question is about it.
- At most {args.max_words} words. Shorter is better when the question is narrow.
- Output the answer text only: no preamble, no explanation, no surrounding quotation marks.

Applicant profile (JSON):
{json.dumps(profile, indent=1)}

Application policy (JSON):
{json.dumps(policy, indent=1)}

Resume (text extracted from the PDF):
{resume.strip() or '(resume text unavailable)'}
"""


def ask_claude(prompt: str, model: str, timeout: int, effort: str = '') -> tuple[str, float]:
    cmd = ['claude', '-p', '--model', model, '--tools', '', '--strict-mcp-config',
           '--disable-slash-commands', '--output-format', 'json']
    if effort:
        cmd.extend(['--effort', effort])
    cmd.append(prompt)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith('{') and '"result"' in line:
            data = json.loads(line)
            if data.get('is_error'):
                raise RuntimeError(f'claude returned an error: {str(data.get("result"))[:300]}')
            return str(data.get('result') or '').strip(), float(data.get('total_cost_usd') or 0.0)
    raise RuntimeError(f'no JSON result from claude (exit {proc.returncode}): {(proc.stderr or proc.stdout)[-400:]}')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--question', required=True, help='the form question, verbatim')
    ap.add_argument('--company', default='')
    ap.add_argument('--title', default='')
    ap.add_argument('--context', default='', help='optional: posting excerpt, answer options, character limit')
    ap.add_argument('--max-words', type=int, default=120)
    ap.add_argument('--model', default=DEFAULT_MODEL)
    ap.add_argument('--effort', default='low', choices=['', 'low', 'medium', 'high'],
                    help="claude --effort; low writes 100-word answers as well as medium at ~half the cost")
    ap.add_argument('--profile', type=Path, default=DEFAULT_PROFILE)
    ap.add_argument('--policy', type=Path, default=DEFAULT_POLICY)
    ap.add_argument('--timeout', type=int, default=240)
    ap.add_argument('--show-cost', action='store_true', help='print the API cost to stderr')
    args = ap.parse_args()

    profile = json.loads(args.profile.read_text(encoding='utf-8'))
    policy = json.loads(args.policy.read_text(encoding='utf-8')) if args.policy.exists() else {}
    resume = resume_text(Path(profile.get('resume_pdf', '')))
    answer, cost = ask_claude(build_prompt(args, profile, policy, resume), args.model, args.timeout, args.effort)
    if args.show_cost:
        print(f'[compose_answer] model={args.model} cost=${cost:.4f}', file=sys.stderr)
    # Strip stray wrapping quotes the model occasionally adds despite the instruction.
    if len(answer) > 1 and answer[0] == answer[-1] and answer[0] in '"\'':
        answer = answer[1:-1].strip()
    print(answer)
    return 2 if answer.startswith(NO_ANSWER) else 0


if __name__ == '__main__':
    sys.exit(main())
