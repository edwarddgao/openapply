"""SmartRecruiters ATS scraper.

API: GET https://api.smartrecruiters.com/v1/companies/{slug}/postings
List endpoint gives metadata only. Detail endpoint needed for descriptions.
Pagination: offset/limit with totalFound.
"""

from __future__ import annotations

import httpx

from .base import ATSScraper, log
from ..normalize import normalize_smartrecruiters


class SmartRecruitersScraper(ATSScraper):
    ats_name = "smartrecruiters"
    max_concurrent = 5
    needs_detail_fetch = True

    def __init__(self):
        super().__init__()
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    async def probe_company(self, slug: str) -> list[dict] | None:
        all_postings = []
        offset = 0
        limit = 100

        while True:
            resp = await self._client.get(
                f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
                params={"offset": offset, "limit": limit},
            )

            if resp.status_code == 404:
                return None

            resp.raise_for_status()
            data = resp.json()

            postings = data.get("content", [])
            if not postings:
                break

            # Normalize without description — caller handles detail fetches
            for p in postings:
                all_postings.append(
                    normalize_smartrecruiters(p, slug, description_html=None)
                )

            total = data.get("totalFound", 0)
            offset += limit
            if offset >= total:
                break

        return all_postings if all_postings else None

    async def fetch_description(self, slug: str, job_id: str) -> str | None:
        resp = await self._client.get(
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{job_id}",
        )

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        data = resp.json()

        # Combine all jobAd sections into one HTML string
        sections = data.get("jobAd", {}).get("sections", {})
        parts = []
        for key in ("jobDescription", "qualifications", "additionalInformation"):
            section = sections.get(key, {})
            text = section.get("text", "")
            if text:
                parts.append(text)

        return "\n".join(parts) if parts else None
