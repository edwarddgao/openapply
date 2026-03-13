"""Abstract base for ATS scrapers."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod

log = logging.getLogger("openapply.scrapers")


class ATSScraper(ABC):
    """Base class for ATS scrapers.

    Subclasses implement probe_company() to fetch all jobs for a company slug.
    The base class provides rate limiting and error handling.
    """

    ats_name: str
    max_concurrent: int = 10
    needs_detail_fetch: bool = False

    def __init__(self):
        self._sem = asyncio.Semaphore(self.max_concurrent)

    @abstractmethod
    async def probe_company(self, slug: str) -> list[dict] | None:
        """Fetch all jobs for a company slug.

        Returns list of normalized job dicts, or None if company is dead/invalid.
        """

    @abstractmethod
    async def fetch_description(self, slug: str, job_id: str) -> str | None:
        """Fetch description HTML for a single job. Returns None if unavailable."""

    async def probe_with_retry(self, slug: str, max_retries: int = 3) -> list[dict] | None:
        """Probe with semaphore + exponential backoff."""
        async with self._sem:
            for attempt in range(max_retries):
                try:
                    return await self.probe_company(slug)
                except Exception as e:
                    if attempt == max_retries - 1:
                        log.warning(f"[{self.ats_name}] {slug}: failed after {max_retries} attempts: {e}")
                        return None
                    wait = 2 ** attempt
                    log.debug(f"[{self.ats_name}] {slug}: retry {attempt + 1} in {wait}s: {e}")
                    await asyncio.sleep(wait)
        return None

    # Per-request delay (seconds) between description fetches to avoid rate limits
    desc_fetch_delay: float = 0.0

    async def fetch_description_with_retry(self, slug: str, job_id: str, max_retries: int = 3) -> str | None:
        """Fetch description with semaphore + exponential backoff."""
        async with self._sem:
            if self.desc_fetch_delay:
                await asyncio.sleep(self.desc_fetch_delay)
            for attempt in range(max_retries):
                try:
                    return await self.fetch_description(slug, job_id)
                except Exception as e:
                    if attempt == max_retries - 1:
                        log.warning(f"[{self.ats_name}] {slug}/{job_id}: description fetch failed: {e}")
                        return None
                    await asyncio.sleep(2 ** attempt)
        return None
