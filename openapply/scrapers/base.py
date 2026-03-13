"""Abstract base for ATS scrapers."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

log = logging.getLogger("openapply.scrapers")


class ATSScraper(ABC):
    """Base class for ATS scrapers.

    Subclasses implement probe_company() to fetch all jobs for a company slug.
    The base class provides rate limiting and error handling.
    """

    ats_name: str
    max_concurrent: int = 10

    def __init__(self):
        self._sem = asyncio.Semaphore(self.max_concurrent)

    @abstractmethod
    async def probe_company(self, slug: str) -> list[dict] | None:
        """Fetch all jobs for a company slug.

        Returns list of normalized job dicts, or None if company is dead/invalid.
        """

    async def probe_with_retry(self, slug: str, max_retries: int = 3) -> list[dict] | None:
        """Probe with semaphore + exponential backoff.

        Returns list of jobs, or None if company is dead (404/empty).
        Raises on transient failures (429/500/timeout) after retries exhausted —
        caller should NOT mark the company as dead.
        """
        async with self._sem:
            for attempt in range(max_retries):
                try:
                    return await self.probe_company(slug)
                except Exception as e:
                    if attempt == max_retries - 1:
                        log.warning(f"[{self.ats_name}] {slug}: failed after {max_retries} attempts: {e}")
                        raise
                    wait = 2 ** attempt
                    log.debug(f"[{self.ats_name}] {slug}: retry {attempt + 1} in {wait}s: {e}")
                    await asyncio.sleep(wait)
        return None
