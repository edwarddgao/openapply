"""Tests for discover.py — CC parsing, Simplify resolution, provenance, merge logic."""

import gzip
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from openapply.discover import (
    find_shard_ranges,
    extract_slugs_from_shard,
    discover_cc,
    _extract_ats_slug,
    is_valid_slug,
    clean_slugs,
    clean_slug_files,
    load_provenance,
    save_provenance,
    update_provenance,
    merge_slugs,
    load_slugs,
    ATS_DOMAINS,
)


# --- Cluster.idx parsing ---

SAMPLE_CLUSTER_IDX = "\n".join([
    "com,anothersite)/page 20260101000000\tcdx-00001.gz\t0\t100\t1",
    "com,ashbyhq,jobs)/clarify/abc123 20260213132646\tcdx-00037.gz\t447690511\t280073\t113158",
    "com,ashbyhq,jobs)/zzz/def456 20260215081909\tcdx-00037.gz\t447970584\t276663\t113159",
    "com,example)/page 20260101000000\tcdx-00050.gz\t0\t100\t200000",
])


class TestFindShardRanges:
    def test_finds_matching_shards(self):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_CLUSTER_IDX
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        shards = find_shard_ranges("CC-MAIN-2026-08", "com,ashbyhq,jobs)", client)
        assert len(shards) == 2
        assert shards[0] == ("cdx-00037.gz", 447690511, 280073)
        assert shards[1] == ("cdx-00037.gz", 447970584, 276663)

    def test_no_matches(self):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_CLUSTER_IDX
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        shards = find_shard_ranges("CC-MAIN-2026-08", "com,nonexistent)", client)
        assert len(shards) == 0

    def test_boundary_match(self):
        """Prefix falls between two entries — should match the boundary block."""
        idx = "\n".join([
            "com,alpha)/page 20260101000000\tcdx-00001.gz\t0\t100\t1",
            "com,gamma)/page 20260101000000\tcdx-00002.gz\t100\t200\t2",
        ])
        mock_resp = MagicMock()
        mock_resp.text = idx
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        shards = find_shard_ranges("CC-MAIN-2026-08", "com,beta)", client)
        assert len(shards) == 1
        assert shards[0] == ("cdx-00002.gz", 100, 200)

    def test_deduplicates_shards(self):
        idx = "\n".join([
            "com,ashbyhq,jobs)/aaa 20260101000000\tcdx-00037.gz\t1000\t200\t1",
            "com,ashbyhq,jobs)/bbb 20260101000000\tcdx-00037.gz\t1000\t200\t2",
        ])
        mock_resp = MagicMock()
        mock_resp.text = idx
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        shards = find_shard_ranges("CC-MAIN-2026-08", "com,ashbyhq,jobs)", client)
        assert len(shards) == 1

    def test_constructs_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.text = "com,ashbyhq,jobs)/x 20260101\tcdx-00001.gz\t0\t100\t1"
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        find_shard_ranges("CC-MAIN-2026-08", "com,ashbyhq,jobs)", client)
        url = client.get.call_args[0][0]
        assert url == "https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2026-08/indexes/cluster.idx"


# --- Slug extraction ---

def _make_gzip_content(lines: list[str]) -> bytes:
    return gzip.compress("\n".join(lines).encode())


class TestExtractSlugsFromShard:
    def test_extracts_greenhouse_slugs(self):
        cdx_lines = [
            'io,greenhouse,job-boards)/acme/jobs/123 20260101 {"url":"https://job-boards.greenhouse.io/acme/jobs/123"}',
            'io,greenhouse,job-boards)/widgetco/jobs/456 20260101 {"url":"https://job-boards.greenhouse.io/widgetco/jobs/456"}',
            'io,greenhouse,job-boards)/acme/jobs/789 20260101 {"url":"https://job-boards.greenhouse.io/acme/jobs/789"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["greenhouse"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2026-08", "cdx-00200.gz", 0, 1000, pattern, client)
        assert slugs == {"acme", "widgetco"}

    def test_extracts_ashby_slugs_with_url_decode(self):
        cdx_lines = [
            'com,ashbyhq,jobs)/my-company/abc 20260101 {"url":"https://jobs.ashbyhq.com/my-company/abc"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["ashby"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2026-08", "cdx-00037.gz", 0, 1000, pattern, client)
        assert "my-company" in slugs

    def test_filters_robots_txt(self):
        cdx_lines = [
            'co,lever,jobs)/robots.txt 20260101 {"url":"https://jobs.lever.co/robots.txt"}',
            'co,lever,jobs)/figma/abc 20260101 {"url":"https://jobs.lever.co/figma/abc"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["lever"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2025-43", "cdx-00027.gz", 0, 1000, pattern, client)
        assert "robots.txt" not in slugs
        assert "figma" in slugs

    def test_filters_non_slug_prefixes(self):
        cdx_lines = [
            'co,lever,jobs)/api/something 20260101 {"url":"https://jobs.lever.co/api/something"}',
            'co,lever,jobs)/static/css 20260101 {"url":"https://jobs.lever.co/static/css"}',
            'co,lever,jobs)/figma/abc 20260101 {"url":"https://jobs.lever.co/figma/abc"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["lever"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2025-43", "cdx-00027.gz", 0, 1000, pattern, client)
        assert slugs == {"figma"}

    def test_filters_embed_and_widget(self):
        cdx_lines = [
            'io,greenhouse,job-boards)/embed 20260101 {"url":"https://job-boards.greenhouse.io/embed"}',
            'io,greenhouse,job-boards)/widget 20260101 {"url":"https://job-boards.greenhouse.io/widget"}',
            'io,greenhouse,job-boards)/discord/jobs/1 20260101 {"url":"https://job-boards.greenhouse.io/discord/jobs/1"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["greenhouse"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2026-08", "cdx-00200.gz", 0, 1000, pattern, client)
        assert slugs == {"discord"}

    def test_lowercases_slugs(self):
        cdx_lines = [
            'co,lever,jobs)/Figma/abc 20260101 {"url":"https://jobs.lever.co/Figma/abc"}',
        ]
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content(cdx_lines)
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["lever"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2025-43", "cdx-00027.gz", 0, 1000, pattern, client)
        assert "figma" in slugs
        assert "Figma" not in slugs

    def test_bad_gzip_returns_empty(self):
        mock_resp = MagicMock()
        mock_resp.content = b"not gzip data"
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["lever"]["url_pattern"]
        slugs = extract_slugs_from_shard("CC-MAIN-2026-08", "cdx-00001.gz", 0, 100, pattern, client)
        assert slugs == set()

    def test_constructs_correct_range_url(self):
        mock_resp = MagicMock()
        mock_resp.content = _make_gzip_content([])
        mock_resp.raise_for_status = MagicMock()

        client = MagicMock()
        client.get.return_value = mock_resp

        pattern = ATS_DOMAINS["ashby"]["url_pattern"]
        extract_slugs_from_shard("CC-MAIN-2026-08", "cdx-00037.gz", 500, 200, pattern, client)
        url = client.get.call_args[0][0]
        headers = client.get.call_args[1].get("headers", {})
        assert url == "https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2026-08/indexes/cdx-00037.gz"
        assert headers["Range"] == "bytes=500-699"


# --- CC discover + merge ---

class TestDiscoverCC:
    def test_merges_with_existing(self, tmp_path):
        existing_file = tmp_path / "greenhouse.txt"
        existing_file.write_text("acme\nwidgetco\n")

        with patch("openapply.discover.get_latest_index", return_value="CC-MAIN-2026-08"), \
             patch("openapply.discover.find_shard_ranges", return_value=[("cdx-00200.gz", 0, 1000)]), \
             patch("openapply.discover.extract_slugs_from_shard", return_value={"acme", "newco", "bigcorp"}):
            result = discover_cc("greenhouse", output_dir=tmp_path)

        assert result == {"acme", "newco", "bigcorp"}
        written = existing_file.read_text().strip().split("\n")
        assert sorted(written) == ["acme", "bigcorp", "newco", "widgetco"]

    def test_skips_empty_shard_files(self):
        with patch("openapply.discover.get_latest_index", return_value="CC-MAIN-2026-08"), \
             patch("openapply.discover.find_shard_ranges", return_value=[("", 0, 0), ("cdx-00001.gz", 0, 100)]), \
             patch("openapply.discover.extract_slugs_from_shard", return_value={"acme"}) as mock_extract:
            result = discover_cc("greenhouse")

        assert result == {"acme"}
        assert mock_extract.call_count == 1

    def test_uses_preferred_index_for_lever(self):
        with patch("openapply.discover.find_shard_ranges", return_value=[]) as mock_find, \
             patch("openapply.discover.get_latest_index") as mock_latest:
            discover_cc("lever")

        mock_latest.assert_not_called()
        assert mock_find.call_args[0][0] == "CC-MAIN-2025-43"

    def test_creates_new_file(self, tmp_path):
        with patch("openapply.discover.get_latest_index", return_value="CC-MAIN-2026-08"), \
             patch("openapply.discover.find_shard_ranges", return_value=[("cdx-00001.gz", 0, 100)]), \
             patch("openapply.discover.extract_slugs_from_shard", return_value={"alpha", "beta"}):
            discover_cc("ashby", output_dir=tmp_path)

        written = (tmp_path / "ashby.txt").read_text().strip().split("\n")
        assert written == ["alpha", "beta"]

    def test_updates_provenance(self, tmp_path):
        prov = {}
        with patch("openapply.discover.get_latest_index", return_value="CC-MAIN-2026-08"), \
             patch("openapply.discover.find_shard_ranges", return_value=[("cdx-00001.gz", 0, 100)]), \
             patch("openapply.discover.extract_slugs_from_shard", return_value={"figma", "stripe"}):
            discover_cc("lever", output_dir=tmp_path, prov=prov)

        assert "lever" in prov
        assert prov["lever"]["figma"] == ["cc"]
        assert prov["lever"]["stripe"] == ["cc"]


# --- Simplify ATS slug extraction ---

class TestExtractAtsSlug:
    def test_lever(self):
        result = _extract_ats_slug("https://jobs.lever.co/figma/abc-123")
        assert result == ("lever", "figma")

    def test_greenhouse(self):
        result = _extract_ats_slug("https://boards.greenhouse.io/discord/jobs/456")
        assert result == ("greenhouse", "discord")

    def test_ashby(self):
        result = _extract_ats_slug("https://jobs.ashbyhq.com/ramp/abc-def")
        assert result == ("ashby", "ramp")

    def test_workday_returns_none(self):
        result = _extract_ats_slug("https://dollartree.wd5.myworkdayjobs.com/dollartreeus/job/blah")
        assert result is None

    def test_unknown_returns_none(self):
        result = _extract_ats_slug("https://careers.example.com/jobs/123")
        assert result is None

    def test_filters_robots_txt(self):
        result = _extract_ats_slug("https://jobs.lever.co/robots.txt")
        assert result is None

    def test_lowercases_slug(self):
        result = _extract_ats_slug("https://jobs.lever.co/Figma/abc-123")
        assert result[1] == "figma"

    def test_url_decodes_slug(self):
        result = _extract_ats_slug("https://jobs.ashbyhq.com/my-company/abc")
        assert result == ("ashby", "my-company")

    def test_rejects_slug_with_spaces(self):
        # URL-decoded %20 becomes space, which is not a valid slug
        result = _extract_ats_slug("https://jobs.ashbyhq.com/my%20company/abc")
        assert result is None


# --- Provenance ---

class TestProvenance:
    def test_save_and_load(self, tmp_path):
        prov = {"lever": {"figma": ["cc", "simplify"]}}
        path = tmp_path / "provenance.json"
        save_provenance(prov, path)
        loaded = load_provenance(path)
        assert loaded == prov

    def test_load_nonexistent(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        assert load_provenance(path) == {}

    def test_update_adds_source(self):
        prov = {}
        update_provenance(prov, "lever", {"figma", "stripe"}, "cc")
        assert prov["lever"]["figma"] == ["cc"]
        assert prov["lever"]["stripe"] == ["cc"]

    def test_update_appends_source(self):
        prov = {"lever": {"figma": ["cc"]}}
        update_provenance(prov, "lever", {"figma"}, "simplify")
        assert prov["lever"]["figma"] == ["cc", "simplify"]

    def test_update_no_duplicate_source(self):
        prov = {"lever": {"figma": ["cc"]}}
        update_provenance(prov, "lever", {"figma"}, "cc")
        assert prov["lever"]["figma"] == ["cc"]



# --- Merge slugs ---

class TestMergeSlugs:
    def test_merges_and_returns_new_count(self, tmp_path):
        (tmp_path / "lever.txt").write_text("existing\n")
        prov = {}
        new_count = merge_slugs("lever", {"existing", "newco"}, "cc", tmp_path, prov)
        assert new_count == 1
        slugs = load_slugs("lever", tmp_path)
        assert slugs == {"existing", "newco"}
        assert prov["lever"]["newco"] == ["cc"]
        assert prov["lever"]["existing"] == ["cc"]

    def test_creates_file_if_missing(self, tmp_path):
        prov = {}
        merge_slugs("ashby", {"ramp", "notion"}, "simplify", tmp_path, prov)
        slugs = load_slugs("ashby", tmp_path)
        assert slugs == {"ramp", "notion"}

    def test_filters_junk_during_merge(self, tmp_path):
        prov = {}
        merge_slugs("lever", {"figma", "1password?department=eng", "a", "12345"}, "cc", tmp_path, prov)
        slugs = load_slugs("lever", tmp_path)
        assert slugs == {"figma"}
        assert "1password?department=eng" not in prov.get("lever", {})


# --- Slug validation ---

class TestIsValidSlug:
    def test_valid(self):
        assert is_valid_slug("figma")
        assert is_valid_slug("1password")
        assert is_valid_slug("air-tek")

    def test_query_params(self):
        assert not is_valid_slug("1password?department=eng")
        assert not is_valid_slug("figma&utm_source=foo")
        assert not is_valid_slug("co=bar")

    def test_too_short(self):
        assert not is_valid_slug("")
        assert not is_valid_slug("a")

    def test_numeric(self):
        assert not is_valid_slug("12345")
        assert not is_valid_slug("113134")

    def test_special_files(self):
        assert not is_valid_slug("robots.txt")
        assert not is_valid_slug("embed")
        assert not is_valid_slug("api-docs")
        assert not is_valid_slug("static-files")

    def test_spaces(self):
        assert not is_valid_slug("my company")
        assert not is_valid_slug("hello world")

    def test_fragments(self):
        assert not is_valid_slug("figma#section")


# --- Clean slug files ---

class TestCleanSlugFiles:
    def test_removes_junk(self, tmp_path):
        (tmp_path / "lever.txt").write_text("figma\n1password?dept=eng\nstripe\na\n12345\n")
        prov = {"lever": {
            "figma": ["simplify"], "1password?dept=eng": ["simplify"],
            "stripe": ["cc"], "a": ["simplify"], "12345": ["simplify"],
        }}
        clean_slug_files(tmp_path, prov)
        slugs = load_slugs("lever", tmp_path)
        assert slugs == {"figma", "stripe"}
        assert "1password?dept=eng" not in prov["lever"]
        assert "a" not in prov["lever"]
        assert "12345" not in prov["lever"]
