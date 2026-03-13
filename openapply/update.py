"""Download jobs.db from the latest GitHub Release.

Usage:
    python -m openapply.update              # download to default location
    python -m openapply.update --db my.db   # custom path
"""

from __future__ import annotations

import argparse
import gzip
import logging
import shutil
from pathlib import Path

import httpx

from .db import DB_PATH

log = logging.getLogger("openapply.update")

GITHUB_REPO = "edwarddgao/openapply"
GITHUB_API = "https://api.github.com"


def get_latest_release_url(repo: str = GITHUB_REPO) -> str | None:
    """Get the download URL for jobs.db.gz from the latest release."""
    resp = httpx.get(
        f"{GITHUB_API}/repos/{repo}/releases/latest",
        headers={"Accept": "application/vnd.github+json"},
        timeout=15,
    )
    if resp.status_code == 404:
        log.error(f"No releases found for {repo}")
        return None
    resp.raise_for_status()

    release = resp.json()
    for asset in release.get("assets", []):
        if asset["name"] == "jobs.db.gz":
            return asset["browser_download_url"]

    log.error(f"No jobs.db.gz asset in release {release.get('tag_name')}")
    return None


def download_db(db_path: Path = DB_PATH, repo: str = GITHUB_REPO) -> bool:
    """Download and decompress jobs.db.gz from the latest GitHub Release.

    Returns True on success.
    """
    url = get_latest_release_url(repo)
    if not url:
        return False

    log.info(f"Downloading {url}...")

    tmp_path = db_path.with_suffix(".db.gz")
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total and downloaded % (1024 * 1024) < 65536:
                    log.info(f"  {downloaded // (1024 * 1024)}MB / {total // (1024 * 1024)}MB")

    log.info("Decompressing...")
    with gzip.open(tmp_path, "rb") as gz, open(db_path, "wb") as out:
        shutil.copyfileobj(gz, out)

    tmp_path.unlink()

    size_mb = db_path.stat().st_size / (1024 * 1024)
    log.info(f"Downloaded jobs.db ({size_mb:.1f}MB) to {db_path}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Download jobs.db from GitHub Releases")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Output path for jobs.db")
    parser.add_argument("--repo", default=GITHUB_REPO, help="GitHub repo (owner/name)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    success = download_db(args.db, args.repo)
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
