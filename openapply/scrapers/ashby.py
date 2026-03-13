"""Ashby ATS scraper.

API: POST https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams
Requires curl_cffi (Cloudflare blocks regular HTTP clients).
Descriptions require separate detail query per job.
"""

from __future__ import annotations

import asyncio
from functools import partial

from curl_cffi import requests as cffi_requests

from .base import ATSScraper, log
from ..normalize import normalize_ashby

LIST_QUERY = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
    jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
        teams { id name parentTeamId }
        jobPostings {
            id title teamId locationName
            workplaceType employmentType
            compensationTierSummary
            secondaryLocations { locationId locationName }
        }
    }
}"""

DETAIL_QUERY = """
query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {
    jobPosting(organizationHostedJobsPageName: $organizationHostedJobsPageName, jobPostingId: $jobPostingId) {
        id title descriptionHtml
    }
}"""

API_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"


def _post_graphql(operation: str, query: str, variables: dict) -> dict:
    """Synchronous GraphQL request via curl_cffi (needed for Cloudflare bypass)."""
    resp = cffi_requests.post(
        f"{API_URL}?op={operation}",
        json={
            "operationName": operation,
            "variables": variables,
            "query": query,
        },
        impersonate="chrome",
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data and not data.get("data"):
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data


class AshbyScraper(ATSScraper):
    ats_name = "ashby"
    max_concurrent = 1
    needs_detail_fetch = True
    probe_delay = 0.2

    async def close(self):
        pass  # curl_cffi doesn't need explicit cleanup

    async def probe_company(self, slug: str) -> list[dict] | None:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            partial(
                _post_graphql,
                "ApiJobBoardWithTeams",
                LIST_QUERY,
                {"organizationHostedJobsPageName": slug},
            ),
        )

        board = data.get("data", {}).get("jobBoard")
        if not board:
            return None  # dead company

        teams_list = board.get("teams", [])
        teams = {t["id"]: t["name"] for t in teams_list}
        postings = board.get("jobPostings", [])

        # Descriptions not available in list query — caller handles detail fetches
        return [normalize_ashby(p, slug, teams, description_html=None) for p in postings]

    async def fetch_description(self, slug: str, job_id: str) -> str | None:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            partial(
                _post_graphql,
                "ApiJobPosting",
                DETAIL_QUERY,
                {
                    "organizationHostedJobsPageName": slug,
                    "jobPostingId": job_id,
                },
            ),
        )

        posting = data.get("data", {}).get("jobPosting")
        if not posting:
            return None
        return posting.get("descriptionHtml")
