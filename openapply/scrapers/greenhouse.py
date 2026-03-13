"""Greenhouse ATS scraper.

API: GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
Returns JSON with jobs array. Descriptions included with ?content=true.
"""

from __future__ import annotations

import httpx

from .base import ATSScraper, log
from ..normalize import normalize_greenhouse


class GreenhouseScraper(ATSScraper):
    ats_name = "greenhouse"
    max_concurrent = 10

    def __init__(self):
        super().__init__()
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    async def probe_company(self, slug: str) -> list[dict] | None:
        resp = await self._client.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
            params={"content": "true"},
        )

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobs", [])

        return [normalize_greenhouse(job, slug) for job in jobs]

    async def fetch_description(self, slug: str, job_id: str) -> str | None:
        # Greenhouse includes descriptions in the list response with ?content=true
        return None
