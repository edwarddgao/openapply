"""Normalize raw ATS responses into unified job dicts for the jobs table."""

from __future__ import annotations

import hashlib
import re
import time
from bs4 import BeautifulSoup


# --- HTML stripping ---

def strip_html(html: str | None) -> str | None:
    if not html:
        return None
    return BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)


# --- Location parsing ---

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

US_STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

CA_PROVINCES = {
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
}


def parse_location(raw: str | None) -> dict:
    """Parse a location string into city, state, country, is_remote."""
    result = {"city": None, "state": None, "country": None, "is_remote": 0}
    if not raw:
        return result

    low = raw.lower().strip()
    if "remote" in low:
        result["is_remote"] = 1

    # Strip common noise
    cleaned = re.sub(r"\(.*?\)", "", raw).strip()
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    # Remove "Remote" / "Hybrid" / "On-site" from parts (already captured in is_remote)
    parts = [p for p in parts if p.lower() not in ("remote", "hybrid", "on-site", "onsite")]

    if not parts:
        return result

    # Check last part for country
    last = parts[-1].strip()
    last_up = last.upper().strip()

    if last_up in ("US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"):
        result["country"] = "US"
        parts = parts[:-1]
    elif last_up == "CANADA":
        result["country"] = "CA"
        parts = parts[:-1]
    elif last_up in ("UK", "UNITED KINGDOM", "ENGLAND", "SCOTLAND", "WALES"):
        result["country"] = "UK"
        parts = parts[:-1]
    elif len(last_up) == 2 and last_up.isalpha():
        # Check US states / CA provinces before treating as country code
        if last_up in US_STATES:
            result["state"] = last_up
            result["country"] = "US"
            result["city"] = parts[-2] if len(parts) >= 2 else None
            return result
        elif last_up in CA_PROVINCES and len(parts) >= 2:
            result["state"] = last_up
            result["country"] = "CA"
            result["city"] = parts[-2] if len(parts) >= 2 else None
            return result
        else:
            result["country"] = last_up
            parts = parts[:-1]

    # Check for state
    if parts:
        maybe_state = parts[-1].strip()
        state_up = maybe_state.upper()
        if state_up in US_STATES:
            result["state"] = state_up
            if not result["country"]:
                result["country"] = "US"
            parts = parts[:-1]
        elif state_up in CA_PROVINCES:
            result["state"] = state_up
            if not result["country"]:
                result["country"] = "CA"
            parts = parts[:-1]
        elif maybe_state.lower() in US_STATE_NAMES:
            result["state"] = US_STATE_NAMES[maybe_state.lower()]
            if not result["country"]:
                result["country"] = "US"
            parts = parts[:-1]

    # Remaining = city
    if parts:
        result["city"] = parts[0]

    return result


# --- Experience level ---

EXPERIENCE_KEYWORDS = {
    "intern": "internship",
    "internship": "internship",
    "entry": "entry",
    "entry level": "entry",
    "entry-level": "entry",
    "new grad": "entry",
    "new college grad": "entry",
    "junior": "junior",
    "jr.": "junior",
    "jr ": "junior",
    "mid": "mid",
    "mid level": "mid",
    "mid-level": "mid",
    "senior": "senior",
    "sr.": "senior",
    "sr ": "senior",
    "staff": "senior",
    "principal": "senior",
    "lead": "senior",
    "director": "executive",
    "vp ": "executive",
    "vice president": "executive",
    "head of": "executive",
}


def parse_experience_level(title: str) -> str:
    """Infer experience level from job title. Returns entry|junior|mid|senior|executive|internship|unknown."""
    low = title.lower()
    for keyword, level in EXPERIENCE_KEYWORDS.items():
        if keyword in low:
            return level
    return "unknown"


# --- Employment type ---

EMPLOYMENT_TYPE_MAP = {
    # Lever
    "full time": "full-time",
    "full-time": "full-time",
    "fulltime": "full-time",
    "part time": "part-time",
    "part-time": "part-time",
    "contract": "contract",
    "contractor": "contract",
    "intern": "internship",
    "internship": "internship",
    "co-op": "internship",
    # Ashby enums
    "FullTime": "full-time",
    "PartTime": "part-time",
    "Intern": "internship",
    "Contract": "contract",
    "Temporary": "contract",
    "permanent": "full-time",
}


def normalize_employment_type(raw: str | None) -> str:
    if not raw:
        return "full-time"
    return EMPLOYMENT_TYPE_MAP.get(raw, EMPLOYMENT_TYPE_MAP.get(raw.lower(), "full-time"))


# --- Content hash ---

def content_hash(title: str, company: str, location: str, description: str | None = None) -> str:
    """Hash for change detection and cross-ATS dedup."""
    parts = [title.lower().strip(), company.lower().strip(), location.lower().strip()]
    if description:
        parts.append(description[:500])
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


# --- Per-ATS normalizers ---

def normalize_lever(raw: dict, slug: str) -> dict:
    """Normalize a Lever posting into a unified job dict."""
    loc_raw = raw.get("categories", {}).get("location", "")
    loc = parse_location(loc_raw)

    # Lever has explicit country and workplaceType
    if raw.get("country"):
        loc["country"] = raw["country"].upper()
    if raw.get("workplaceType") == "remote":
        loc["is_remote"] = 1

    salary = raw.get("salaryRange", {})
    min_sal = salary.get("min")
    max_sal = salary.get("max")

    # Combine description parts
    desc_parts = [raw.get("descriptionPlain", "")]
    for lst in raw.get("lists", []):
        text = lst.get("text", "")
        content = strip_html(lst.get("content", ""))
        if text:
            desc_parts.append(text)
        if content:
            desc_parts.append(content)
    desc_parts.append(raw.get("additionalPlain", ""))
    description = "\n\n".join(p for p in desc_parts if p and p.strip())

    cats = raw.get("categories", {})
    title = raw.get("text", "")

    return {
        "job_id": f"lever:{raw['id']}",
        "ats": "lever",
        "company_id": f"lever:{slug}",
        "ats_job_id": raw["id"],
        "title": title,
        "company_name": None,  # Lever API doesn't include company name; set by scrape.py
        "description_text": description or None,
        "location_raw": loc_raw,
        "city": loc["city"],
        "state": loc["state"],
        "country": loc["country"],
        "is_remote": loc["is_remote"],
        "department": cats.get("team") or cats.get("department"),
        "employment_type": normalize_employment_type(cats.get("commitment")),
        "experience_level": parse_experience_level(title),
        "min_salary": min_sal,
        "max_salary": max_sal,
        "apply_url": raw.get("applyUrl", f"https://jobs.lever.co/{slug}/{raw['id']}/apply"),
        "now": int(time.time()),
        "content_hash": content_hash(title, slug, loc_raw, description),
    }


def normalize_greenhouse(raw: dict, slug: str) -> dict:
    """Normalize a Greenhouse job into a unified job dict."""
    loc_raw = raw.get("location", {}).get("name", "")
    loc = parse_location(loc_raw)

    # Check offices for remote hint
    for office in raw.get("offices", []):
        if "remote" in office.get("name", "").lower():
            loc["is_remote"] = 1

    description = strip_html(raw.get("content"))
    title = raw.get("title", "")
    depts = raw.get("departments", [])
    department = depts[0]["name"] if depts else None

    return {
        "job_id": f"greenhouse:{raw['id']}",
        "ats": "greenhouse",
        "company_id": f"greenhouse:{slug}",
        "ats_job_id": str(raw["id"]),
        "title": title,
        "company_name": raw.get("company_name"),  # Greenhouse includes this per-job
        "description_text": description,
        "location_raw": loc_raw,
        "city": loc["city"],
        "state": loc["state"],
        "country": loc["country"],
        "is_remote": loc["is_remote"],
        "department": department,
        "employment_type": "full-time",  # Greenhouse doesn't expose this
        "experience_level": parse_experience_level(title),
        "min_salary": None,  # Greenhouse doesn't expose salary in public API
        "max_salary": None,
        "apply_url": raw.get("absolute_url", f"https://job-boards.greenhouse.io/{slug}/jobs/{raw['id']}"),
        "now": int(time.time()),
        "content_hash": content_hash(title, slug, loc_raw, description),
    }


