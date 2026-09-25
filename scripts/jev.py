"""Seniority classification with TypeSafe Jev (a System One decision model).

Shared by build_shortlist.py (--jev) and eval_jev_seniority.py. Jev answers a
typed Choice question with per-option probabilities and a confidence score; see
https://docs.typesafe.ai/api. It is reached through OpenRouter or TypeSafe
directly, whichever key is set (shell env first, then .env).
"""
from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable

import httpx


LABELS = ('entry', 'mid', 'senior')
CRITERIA = {
    'entry': 'New grad or early career: 0-2 years of experience expected, or the posting says junior, associate, new grad, or early career.',
    'mid': 'Experienced individual contributor: roughly 2-5 years expected, works independently, but no senior, staff, or lead scope.',
    'senior': 'Senior level: 5+ years expected, or the role leads a team, owns architecture or technical direction, or mentors other engineers.',
}
INSTRUCTIONS = 'What seniority level is the job in `job` hiring for?'

ENDPOINTS = {
    'OPENROUTER_API_KEY': ('https://openrouter.ai/api/v1/systemone', 'typesafe/jev-1.13'),
    'TYPESAFE_API_KEY': ('https://api.typesafe.ai/v1/systemone', 'jev-1.13'),
}

# Jev's accuracy drops as state fills with unrelated text, so it gets the
# requirements part of a posting rather than the whole thing. Anchors are tried in
# order: explicit headings first, looser phrasing only when no heading exists
# ("you have" also appears in culture and perks blurbs).
EXCERPT_CHARS = 3000
REQ_ANCHOR_RES = [
    re.compile(r"(?i)\b(requirements|qualifications|what you(?:'ll)? (?:need|bring)|who you are|about you|what we(?:'re)? looking for)\b"),
    re.compile(r"(?i)\b(you (?:have|bring|might be)|you should have|experience with)\b"),
]


def excerpt(description: str) -> str:
    for anchor in REQ_ANCHOR_RES:
        if match := anchor.search(description):
            return description[match.start():match.start() + EXCERPT_CHARS]
    return description[:EXCERPT_CHARS]


def job_state(title: str, department: str, description: str) -> dict[str, str]:
    return {'title': title, 'department': department, 'requirements_excerpt': excerpt(description)}


def load_dotenv(path: Path = Path(__file__).resolve().parent.parent / '.env') -> None:
    # Keys already exported in the shell win over .env.
    if not path.exists():
        return
    for line in path.read_text(encoding='utf-8').splitlines():
        name, sep, value = line.strip().removeprefix('export ').partition('=')
        if sep and name and not name.startswith('#'):
            os.environ.setdefault(name.strip(), value.strip().strip('"\''))


class Jev:
    def __init__(self, timeout: float = 30) -> None:
        load_dotenv()
        key_name = next((name for name in ENDPOINTS if os.environ.get(name)), None)
        if not key_name:
            raise RuntimeError('Set OPENROUTER_API_KEY or TYPESAFE_API_KEY (shell or .env)')
        self.url, self.model = ENDPOINTS[key_name]
        self.client = httpx.Client(headers={'Authorization': f'Bearer {os.environ[key_name]}'}, timeout=timeout)

    def seniority(self, state: dict[str, Any]) -> dict[str, Any]:
        body = {
            'model': self.model,
            'state': {'job': state},
            'questions': {'seniority': {'type': 'choice', 'instructions': INSTRUCTIONS, 'criteria': CRITERIA}},
        }
        for attempt in range(6):
            response = self.client.post(self.url, json=body)
            # 429 rate limit and 529 overload are documented as retryable.
            if response.status_code in (429, 529) or response.status_code >= 500:
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            answer = response.json()['answers']['seniority']
            return {'label': answer['choice'], 'confidence': answer['confidence'], 'probabilities': answer['probabilities']}
        response.raise_for_status()
        raise RuntimeError(f'Jev kept returning {response.status_code}')

    def seniority_many(self, states: Iterable[dict[str, Any]], workers: int = 8) -> list[dict[str, Any] | Exception]:
        """One result per state, in order; a failed call yields its exception instead."""
        def one(state: dict[str, Any]) -> dict[str, Any] | Exception:
            try:
                return self.seniority(state)
            except Exception as exc:
                return exc

        with ThreadPoolExecutor(workers) as pool:
            return list(pool.map(one, states))
