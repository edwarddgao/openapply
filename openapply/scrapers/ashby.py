"""Ashby ATS scraper.

API: GET https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true
Returns JSON with jobs array. Descriptions and compensation included in list response.
"""

from __future__ import annotations

import httpx

from .base import ATSScraper, log
from ..normalize import normalize_ashby


class AshbyScraper(ATSScraper):
    ats_name = "ashby"
    max_concurrent = 5

    def __init__(self):
        super().__init__()
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self._client.aclose()

    async def probe_company(self, slug: str) -> list[dict] | None:
        resp = await self._client.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
            params={"includeCompensation": "true"},
        )

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobs", [])

        if not jobs:
            return None

        # Build teams dict from job fields (REST API includes team/department per job)
        teams = {}
        for j in jobs:
            # REST API gives team name directly, no teamId lookup needed
            pass

        return [
            normalize_ashby_rest(j, slug)
            for j in jobs
            if j.get("isListed", True)
        ]

    async def fetch_description(self, slug: str, job_id: str) -> str | None:
        # Descriptions included in list response — no separate fetch needed
        return None


def normalize_ashby_rest(raw: dict, slug: str) -> dict:
    """Normalize an Ashby REST API job posting into a unified job dict."""
    from ..normalize import (
        parse_location, normalize_employment_type,
        parse_experience_level, content_hash, strip_html,
    )
    import time

    loc_raw = raw.get("location", "")
    loc = parse_location(loc_raw)

    if raw.get("isRemote"):
        loc["is_remote"] = 1
    if raw.get("workplaceType") == "Remote":
        loc["is_remote"] = 1

    # Structured address fallback
    addr = raw.get("address", {}).get("postalAddress", {})
    if addr:
        if not loc["city"] and addr.get("addressLocality"):
            loc["city"] = addr["addressLocality"]
        if not loc["state"] and addr.get("addressRegion"):
            loc["state"] = addr["addressRegion"]
        if not loc["country"] and addr.get("addressCountry"):
            country = addr["addressCountry"]
            if country in ("USA", "US"):
                loc["country"] = "US"
            elif country in ("CAN", "CA", "Canada"):
                loc["country"] = "CA"
            elif len(country) == 2:
                loc["country"] = country.upper()

    # Compensation from structured data
    min_sal, max_sal = None, None
    comp = raw.get("compensation", {})
    for sc in comp.get("summaryComponents", []):
        if sc.get("compensationType") == "Salary":
            min_sal = sc.get("minValue")
            max_sal = sc.get("maxValue")
            break

    description = raw.get("descriptionPlain") or strip_html(raw.get("descriptionHtml"))
    title = raw.get("title", "").strip()
    department = raw.get("department")
    team = raw.get("team")

    return {
        "job_id": f"ashby:{raw['id']}",
        "ats": "ashby",
        "company_id": f"ashby:{slug}",
        "ats_job_id": raw["id"],
        "title": title,
        "company_name": None,
        "description_text": description,
        "location_raw": loc_raw,
        "city": loc["city"],
        "state": loc["state"],
        "country": loc["country"],
        "is_remote": loc["is_remote"],
        "department": department or team,
        "employment_type": normalize_employment_type(raw.get("employmentType")),
        "experience_level": parse_experience_level(title),
        "min_salary": min_sal,
        "max_salary": max_sal,
        "apply_url": raw.get("applyUrl", f"https://jobs.ashbyhq.com/{slug}/{raw['id']}/application"),
        "now": int(time.time()),
        "content_hash": content_hash(title, slug, loc_raw, description),
    }
