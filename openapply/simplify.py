"""Simplify.jobs Typesense search client. Dev dependency for validation only."""

from __future__ import annotations

import json as _json
from typing import Any

import httpx

TYPESENSE_SEARCH = "https://js-ha.simplify.jobs/multi_search"
TYPESENSE_API_KEY = "SWF1ODFZbzBkcVlVdnVwT2FqUE5EZ3JpSk5hVmdpUHg1SklXWEdGbHZVRT1POHJieyJleGNsdWRlX2ZpZWxkcyI6ImNvbXBhbnlfdXJsLGNhdGVnb3JpZXMsYWRkaXRpb25hbF9yZXF1aXJlbWVudHMsY291bnRyaWVzLGRlZ3JlZXMsZ2VvbG9jYXRpb25zLGluZHVzdHJpZXMsaXNfc2ltcGxlX2FwcGxpY2F0aW9uLGpvYl9saXN0cyxsZWFkZXJzaGlwX3R5cGUsc2VjdXJpdHlfY2xlYXJhbmNlLHNraWxscyx1cmwifQ=="
TYPESENSE_COLLECTION = "jobs"


def _typesense_post(
    payload: dict[str, Any], *, api_key: str = TYPESENSE_API_KEY
) -> dict[str, Any]:
    resp = httpx.post(
        TYPESENSE_SEARCH,
        params={"x-typesense-api-key": api_key},
        content=_json.dumps(payload),
        headers={
            "Content-Type": "text/plain",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Referer": "https://simplify.jobs/",
        },
        timeout=15.0,
    )
    resp.raise_for_status()
    return resp.json()["results"][0]


def search_jobs(
    *,
    query: str = "*",
    filter_by: str = "",
    page: int = 1,
    per_page: int = 250,
) -> dict[str, Any]:
    search_params: dict[str, Any] = {
        "q": query,
        "query_by": "title,company_name",
        "sort_by": "_text_match:desc",
        "page": page,
        "per_page": per_page,
    }
    if filter_by:
        search_params["filter_by"] = filter_by

    return _typesense_post({"searches": [{"collection": TYPESENSE_COLLECTION, **search_params}]})


def count_company_jobs(company_name: str) -> int:
    """Count jobs for a company in Simplify."""
    result = search_jobs(
        query="*",
        filter_by=f"company_name:=[`{company_name}`]",
        per_page=0,
    )
    return result.get("found", 0)
