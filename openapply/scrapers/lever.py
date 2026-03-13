"""Lever ATS scraper.

API: GET https://api.lever.co/v0/postings/{slug}
Returns JSON array of postings. Descriptions included in list response.
"""

from __future__ import annotations

import httpx

from .base import ATSScraper, log
from ..normalize import normalize_lever


class LeverScraper(ATSScraper):
    ats_name = "lever"
    max_concurrent = 10

    def __init__(self):
        super().__init__()
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    async def probe_company(self, slug: str) -> list[dict] | None:
        resp = await self._client.get(f"https://api.lever.co/v0/postings/{slug}")

        if resp.status_code == 404:
            return None  # dead company

        resp.raise_for_status()
        data = resp.json()

        # Lever returns error dict for invalid slugs
        if isinstance(data, dict):
            return None

        return [normalize_lever(posting, slug) for posting in data]

