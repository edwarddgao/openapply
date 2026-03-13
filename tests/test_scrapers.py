"""Tests for scrapers — using saved fixtures, no live API calls."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from openapply.scrapers.lever import LeverScraper
from openapply.scrapers.greenhouse import GreenhouseScraper
from openapply.scrapers.ashby import AshbyScraper
from openapply.scrapers.smartrecruiters import SmartRecruitersScraper

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lever_response():
    return json.loads((FIXTURES / "lever_15five.json").read_text())


@pytest.fixture
def greenhouse_response():
    return json.loads((FIXTURES / "greenhouse_discord.json").read_text())


@pytest.fixture
def ashby_response():
    return json.loads((FIXTURES / "ashby_ramp.json").read_text())


@pytest.fixture
def sr_list_response():
    return json.loads((FIXTURES / "smartrecruiters_visa_list.json").read_text())


@pytest.fixture
def sr_detail_response():
    return json.loads((FIXTURES / "smartrecruiters_visa_detail.json").read_text())


class TestLeverScraper:
    @pytest.mark.asyncio
    async def test_probe_company(self, lever_response):
        scraper = LeverScraper()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = lever_response
        mock_resp.raise_for_status = MagicMock()

        scraper._client.get = AsyncMock(return_value=mock_resp)

        result = await scraper.probe_company("15five")
        assert result is not None
        assert len(result) == len(lever_response)
        assert all(j["ats"] == "lever" for j in result)
        assert all(j["apply_url"] for j in result)
        await scraper.close()

    @pytest.mark.asyncio
    async def test_probe_dead_company(self):
        scraper = LeverScraper()
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        scraper._client.get = AsyncMock(return_value=mock_resp)

        result = await scraper.probe_company("nonexistent")
        assert result is None
        await scraper.close()


class TestGreenhouseScraper:
    @pytest.mark.asyncio
    async def test_probe_company(self, greenhouse_response):
        scraper = GreenhouseScraper()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = greenhouse_response
        mock_resp.raise_for_status = MagicMock()

        scraper._client.get = AsyncMock(return_value=mock_resp)

        result = await scraper.probe_company("discord")
        assert result is not None
        assert len(result) == len(greenhouse_response["jobs"])
        assert all(j["ats"] == "greenhouse" for j in result)
        assert all(j["description_text"] for j in result)
        await scraper.close()


class TestAshbyScraper:
    @pytest.mark.asyncio
    async def test_probe_company(self, ashby_response):
        scraper = AshbyScraper()

        with patch("openapply.scrapers.ashby._post_graphql", return_value=ashby_response):
            result = await scraper.probe_company("ramp")

        assert result is not None
        postings = ashby_response["data"]["jobBoard"]["jobPostings"]
        assert len(result) == len(postings)
        assert all(j["ats"] == "ashby" for j in result)
        assert all(j["apply_url"].startswith("https://jobs.ashbyhq.com/ramp/") for j in result)
        # Descriptions are None from list query (fetched separately)
        assert all(j["description_text"] is None for j in result)

    @pytest.mark.asyncio
    async def test_probe_dead_company(self):
        scraper = AshbyScraper()
        dead_response = {"data": {"jobBoard": None}}

        with patch("openapply.scrapers.ashby._post_graphql", return_value=dead_response):
            result = await scraper.probe_company("nonexistent")

        assert result is None


class TestSmartRecruitersScraper:
    @pytest.mark.asyncio
    async def test_probe_company(self, sr_list_response):
        scraper = SmartRecruitersScraper()

        # Make pagination stop after first page by setting totalFound = len(content)
        sr_list_response["totalFound"] = len(sr_list_response["content"])

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sr_list_response
        mock_resp.raise_for_status = MagicMock()

        scraper._client.get = AsyncMock(return_value=mock_resp)

        result = await scraper.probe_company("VISA")
        assert result is not None
        assert len(result) == len(sr_list_response["content"])
        assert all(j["ats"] == "smartrecruiters" for j in result)
        await scraper.close()

    @pytest.mark.asyncio
    async def test_fetch_description(self, sr_detail_response):
        scraper = SmartRecruitersScraper()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sr_detail_response
        mock_resp.raise_for_status = MagicMock()

        scraper._client.get = AsyncMock(return_value=mock_resp)

        desc = await scraper.fetch_description("VISA", "744000114532207")
        assert desc is not None
        assert len(desc) > 100
        await scraper.close()
