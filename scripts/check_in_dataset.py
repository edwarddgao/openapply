#!/usr/bin/env python3
"""Check whether company names (e.g. surfaced from X hiring posts) are in our dataset.

Usage:
  echo "Acme AI\nVercel\n1Password" | python scripts/check_in_dataset.py
  python scripts/check_in_dataset.py "Acme AI" "Vercel"

A company is considered IN the dataset if its normalized name matches a slug we
scrape (universe) — we additionally report whether that slug has live postings.
Normalization strips everything but [a-z0-9] and also tries a few common company
suffix-stripped variants (ai, hq, labs, inc, io, app, the-).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

INDEX = Path(".cache/slug_index.json")

SUFFIXES = ("technologies", "technology", "labs", "lab", "inc", "llc", "ai", "io",
            "hq", "app", "co", "corp", "software", "systems", "health", "bio")
PREFIXES = ("the", "get", "join", "try", "use", "with", "go")


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def variants(name: str) -> list[str]:
    base = norm(name)
    out = {base}
    for suf in SUFFIXES:
        if base.endswith(suf) and len(base) > len(suf) + 2:
            out.add(base[: -len(suf)])
    for pre in PREFIXES:
        if base.startswith(pre) and len(base) > len(pre) + 2:
            out.add(base[len(pre):])
    # also collapse "and" / "&"
    out.add(re.sub(r"and", "", base))
    return [v for v in out if v]


def main() -> None:
    if not INDEX.exists():
        sys.exit("Missing .cache/slug_index.json — build it first.")
    idx = json.loads(INDEX.read_text())
    universe = set(idx["universe"])
    live = set(idx["live"])

    names = sys.argv[1:]
    if not names:
        names = [ln.strip() for ln in sys.stdin if ln.strip()]

    for name in names:
        hit = ""
        status = "MISSING"
        for v in variants(name):
            if v in universe:
                hit = v
                status = "IN-DATASET-LIVE" if v in live else "IN-UNIVERSE-NO-LIVE"
                break
        print(f"{status}\t{name}\t{hit}")


if __name__ == "__main__":
    main()
