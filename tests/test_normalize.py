"""Tests for normalize.py — location parsing, experience level, salary, per-ATS normalizers."""

import json
from pathlib import Path

import pytest

from openapply.normalize import (
    parse_location,
    parse_experience_level,
    normalize_employment_type,
    parse_ashby_compensation,
    normalize_sr_experience,
    strip_html,
    content_hash,
    normalize_lever,
    normalize_greenhouse,
    normalize_ashby,
    normalize_smartrecruiters,
)

FIXTURES = Path(__file__).parent / "fixtures"


# --- Location parsing ---

class TestParseLocation:
    def test_us_city_state(self):
        result = parse_location("San Francisco, CA")
        assert result["city"] == "San Francisco"
        assert result["state"] == "CA"
        assert result["country"] == "US"

    def test_us_city_state_country(self):
        result = parse_location("New York, NY, US")
        assert result["city"] == "New York"
        assert result["state"] == "NY"
        assert result["country"] == "US"

    def test_us_state_name(self):
        result = parse_location("Austin, Texas")
        assert result["city"] == "Austin"
        assert result["state"] == "TX"
        assert result["country"] == "US"

    def test_remote(self):
        result = parse_location("US, Remote")
        assert result["is_remote"] == 1
        assert result["country"] == "US"

    def test_canada(self):
        result = parse_location("Toronto, ON, Canada")
        assert result["city"] == "Toronto"
        assert result["state"] == "ON"
        assert result["country"] == "CA"

    def test_uk(self):
        result = parse_location("London, England, United Kingdom")
        assert result["city"] == "London"
        assert result["country"] == "UK"

    def test_country_code_only(self):
        result = parse_location("Jakarta, INDONESIA, Indonesia")
        assert result["city"] == "Jakarta"

    def test_none(self):
        result = parse_location(None)
        assert result == {"city": None, "state": None, "country": None, "is_remote": 0}

    def test_empty(self):
        result = parse_location("")
        assert result == {"city": None, "state": None, "country": None, "is_remote": 0}

    def test_parenthetical_stripped(self):
        result = parse_location("New York, NY (HQ)")
        assert result["city"] == "New York"
        assert result["state"] == "NY"


# --- Experience level ---

class TestParseExperienceLevel:
    def test_senior(self):
        assert parse_experience_level("Senior Software Engineer") == "senior"

    def test_entry(self):
        assert parse_experience_level("Entry Level Developer") == "entry"

    def test_intern(self):
        assert parse_experience_level("Software Engineering Intern") == "internship"

    def test_new_grad(self):
        assert parse_experience_level("New Grad Software Engineer") == "entry"

    def test_staff(self):
        assert parse_experience_level("Staff Engineer") == "senior"

    def test_unknown(self):
        assert parse_experience_level("Software Engineer") == "unknown"

    def test_director(self):
        assert parse_experience_level("Director of Engineering") == "executive"


# --- Employment type ---

class TestNormalizeEmploymentType:
    def test_full_time(self):
        assert normalize_employment_type("Full Time") == "full-time"

    def test_ashby_enum(self):
        assert normalize_employment_type("FullTime") == "full-time"
        assert normalize_employment_type("PartTime") == "part-time"
        assert normalize_employment_type("Intern") == "internship"
        assert normalize_employment_type("Contract") == "contract"

    def test_sr_permanent(self):
        assert normalize_employment_type("permanent") == "full-time"

    def test_none_defaults(self):
        assert normalize_employment_type(None) == "full-time"


# --- Ashby salary ---

class TestParseAshbyCompensation:
    def test_standard(self):
        mn, mx = parse_ashby_compensation("$138.8K – $212.1K • Offers Equity")
        assert mn == 138800.0
        assert mx == 212100.0

    def test_none(self):
        assert parse_ashby_compensation(None) == (None, None)

    def test_no_match(self):
        assert parse_ashby_compensation("Offers Equity") == (None, None)


# --- SmartRecruiters experience ---

class TestNormalizeSRExperience:
    def test_mid_senior(self):
        assert normalize_sr_experience("mid_senior_level") == "mid"

    def test_entry(self):
        assert normalize_sr_experience("entry_level") == "entry"

    def test_internship(self):
        assert normalize_sr_experience("internship") == "internship"

    def test_none(self):
        assert normalize_sr_experience(None) == "unknown"


# --- HTML stripping ---

class TestStripHtml:
    def test_basic(self):
        result = strip_html("<p>Hello <b>world</b></p>")
        assert "Hello" in result
        assert "world" in result

    def test_none(self):
        assert strip_html(None) is None

    def test_empty(self):
        assert strip_html("") is None


# --- Content hash ---

class TestContentHash:
    def test_deterministic(self):
        h1 = content_hash("Engineer", "Acme", "NYC")
        h2 = content_hash("Engineer", "Acme", "NYC")
        assert h1 == h2

    def test_different_inputs(self):
        h1 = content_hash("Engineer", "Acme", "NYC")
        h2 = content_hash("Manager", "Acme", "NYC")
        assert h1 != h2



# --- Per-ATS normalizers with fixtures ---

class TestNormalizeLever:
    @pytest.fixture
    def posting(self):
        data = json.loads((FIXTURES / "lever_15five.json").read_text())
        return data[0]

    def test_fields(self, posting):
        result = normalize_lever(posting, "15five")
        assert result["ats"] == "lever"
        assert result["job_id"].startswith("lever:")
        assert result["title"] == posting["text"]
        assert result["employment_type"] == "full-time"
        assert result["apply_url"].startswith("https://jobs.lever.co/")
        assert result["description_text"]
        assert len(result["description_text"]) > 100

    def test_salary(self, posting):
        result = normalize_lever(posting, "15five")
        assert result["min_salary"] is not None
        assert result["max_salary"] is not None
        assert result["max_salary"] > result["min_salary"]


class TestNormalizeGreenhouse:
    @pytest.fixture
    def job(self):
        data = json.loads((FIXTURES / "greenhouse_discord.json").read_text())
        return data["jobs"][0]

    def test_fields(self, job):
        result = normalize_greenhouse(job, "discord")
        assert result["ats"] == "greenhouse"
        assert result["job_id"].startswith("greenhouse:")
        assert result["title"] == job["title"]
        assert result["company_name"] == "Discord"
        assert result["apply_url"].startswith("https://")
        assert result["description_text"]
        assert result["department"]


class TestNormalizeAshby:
    @pytest.fixture
    def data(self):
        return json.loads((FIXTURES / "ashby_ramp.json").read_text())

    def test_fields(self, data):
        posting = data["data"]["jobBoard"]["jobPostings"][0]
        teams_list = data["data"]["jobBoard"]["teams"]
        teams = {t["id"]: t["name"] for t in teams_list}
        result = normalize_ashby(posting, "ramp", teams)
        assert result["ats"] == "ashby"
        assert result["job_id"].startswith("ashby:")
        assert result["title"] == posting["title"]
        assert result["department"]
        assert result["employment_type"] == "full-time"
        assert result["apply_url"].startswith("https://jobs.ashbyhq.com/ramp/")
        assert result["min_salary"] is not None


class TestNormalizeSmartRecruiters:
    @pytest.fixture
    def posting(self):
        return json.loads((FIXTURES / "smartrecruiters_visa_detail.json").read_text())

    def test_fields(self, posting):
        sections = posting.get("jobAd", {}).get("sections", {})
        desc_html = "\n".join(
            s.get("text", "") for s in sections.values() if s.get("text")
        )
        result = normalize_smartrecruiters(posting, "VISA", desc_html)
        assert result["ats"] == "smartrecruiters"
        assert result["job_id"].startswith("smartrecruiters:")
        assert result["title"] == posting["name"]
        assert result["company_name"] == "Visa"
        assert result["city"]
        assert result["country"]
        assert result["department"]
        assert result["apply_url"].startswith("https://")
        assert result["description_text"]
