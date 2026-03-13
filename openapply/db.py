"""Database schema, initialization, and helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "jobs.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    company_id TEXT PRIMARY KEY,  -- "{ats}:{slug}"
    slug TEXT NOT NULL,
    ats TEXT NOT NULL,
    name TEXT,
    last_probed_at INTEGER,
    is_dead INTEGER DEFAULT 0,
    UNIQUE(ats, slug)
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,       -- "{ats}:{native_id}"
    ats TEXT NOT NULL,
    company_id TEXT NOT NULL,
    ats_job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company_name TEXT,
    description_text TEXT,
    location_raw TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    is_remote INTEGER DEFAULT 0,
    department TEXT,
    employment_type TEXT,
    experience_level TEXT,
    min_salary REAL,
    max_salary REAL,
    apply_url TEXT NOT NULL,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    content_hash TEXT,
    UNIQUE(ats, ats_job_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_jobs_ats ON jobs(ats);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_at);

CREATE TABLE IF NOT EXISTS candidates (
    job_id TEXT PRIMARY KEY,
    ats TEXT NOT NULL,
    title TEXT NOT NULL,
    company_name TEXT,
    description_text TEXT,
    location_raw TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    is_remote INTEGER DEFAULT 0,
    department TEXT,
    employment_type TEXT,
    experience_level TEXT,
    min_salary REAL,
    max_salary REAL,
    apply_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applications (
    job_id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exclusions (
    job_id TEXT PRIMARY KEY,
    reason TEXT,
    company TEXT,
    title TEXT,
    url TEXT,
    excluded_at TEXT NOT NULL,
    block_type TEXT DEFAULT 'platform'
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Open a connection with WAL mode and row factory."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Create all tables if they don't exist."""
    conn = get_connection(db_path)
    conn.executescript(SCHEMA)
    conn.close()


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def upsert_company(conn: sqlite3.Connection, company: dict) -> None:
    conn.execute(
        """INSERT INTO companies (company_id, slug, ats, name, last_probed_at, is_dead)
           VALUES (:company_id, :slug, :ats, :name, :last_probed_at, 0)
           ON CONFLICT(company_id) DO UPDATE SET
               name = COALESCE(:name, companies.name),
               last_probed_at = :last_probed_at,
               is_dead = 0""",
        company,
    )


def upsert_job(conn: sqlite3.Connection, job: dict) -> None:
    conn.execute(
        """INSERT INTO jobs (
               job_id, ats, company_id, ats_job_id, title, company_name,
               description_text, location_raw, city, state, country, is_remote,
               department, employment_type, experience_level,
               min_salary, max_salary, apply_url,
               first_seen_at, last_seen_at, content_hash
           ) VALUES (
               :job_id, :ats, :company_id, :ats_job_id, :title, :company_name,
               :description_text, :location_raw, :city, :state, :country, :is_remote,
               :department, :employment_type, :experience_level,
               :min_salary, :max_salary, :apply_url,
               :now, :now, :content_hash
           )
           ON CONFLICT(job_id) DO UPDATE SET
               title = :title,
               company_name = :company_name,
               description_text = COALESCE(:description_text, jobs.description_text),
               location_raw = :location_raw,
               city = :city, state = :state, country = :country,
               is_remote = :is_remote,
               department = :department,
               employment_type = :employment_type,
               experience_level = :experience_level,
               min_salary = :min_salary, max_salary = :max_salary,
               apply_url = :apply_url,
               last_seen_at = :now,
               content_hash = :content_hash""",
        job,
    )


def mark_dead_company(conn: sqlite3.Connection, company_id: str) -> None:
    conn.execute(
        "UPDATE companies SET is_dead = 1 WHERE company_id = ?",
        (company_id,),
    )


def purge_stale_jobs(conn: sqlite3.Connection, cutoff_ts: int) -> int:
    """Delete jobs not seen since cutoff. Returns count deleted."""
    cur = conn.execute(
        "DELETE FROM jobs WHERE last_seen_at < ?", (cutoff_ts,)
    )
    return cur.rowcount


def job_exists(conn: sqlite3.Connection, job_id: str) -> dict | None:
    """Return existing job's content_hash and description presence, or None."""
    row = conn.execute(
        "SELECT content_hash, description_text IS NOT NULL as has_desc FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return dict(row) if row else None


def batch_job_exists(conn: sqlite3.Connection, job_ids: list[str]) -> dict[str, dict]:
    """Return {job_id: {content_hash, has_desc}} for existing jobs."""
    if not job_ids:
        return {}
    placeholders = ",".join("?" * len(job_ids))
    rows = conn.execute(
        f"SELECT job_id, content_hash, description_text IS NOT NULL as has_desc FROM jobs WHERE job_id IN ({placeholders})",
        job_ids,
    ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}
