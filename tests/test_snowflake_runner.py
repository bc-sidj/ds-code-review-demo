"""Tests for snowflake_runner.py — query parsing and graceful fallback."""

import os
import sys
import pathlib
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

from snowflake_runner import SnowflakeResult, parse_sql_queries, run_queries


class TestParseQueries:
    """Test SQL query parsing from AI-generated text."""

    def test_simple_query(self):
        sql = "-- Row count check\nSELECT COUNT(*) FROM FIL.STORE;"
        queries = parse_sql_queries(sql)
        assert len(queries) == 1
        assert queries[0]["name"] == "Row count check"
        assert "COUNT(*)" in queries[0]["sql"]

    def test_multiple_queries(self):
        sql = """\
-- Category 1
-- Row count
SELECT COUNT(*) FROM FIL.STORE;
-- Hash check
SELECT HASH_AGG(*) FROM FIL.STORE;
"""
        queries = parse_sql_queries(sql)
        assert len(queries) == 2

    def test_category_detection(self):
        sql = """\
-- Category 3
-- Regression check
SELECT COUNT(*) FROM FIL.STORE;
-- Category 5
-- Downstream check
SELECT * FROM security.table_usage_summary;
"""
        queries = parse_sql_queries(sql)
        assert queries[0]["category"] == 3
        assert queries[1]["category"] == 5

    def test_empty_input(self):
        assert parse_sql_queries("") == []
        assert parse_sql_queries("   ") == []

    def test_markdown_fences_ignored(self):
        sql = "```sql\nSELECT 1;\n```"
        queries = parse_sql_queries(sql)
        assert len(queries) == 1

    def test_comment_only(self):
        sql = "-- Just a comment\n-- Another comment"
        queries = parse_sql_queries(sql)
        assert len(queries) == 0

    def test_show_describe_queries(self):
        sql = "-- Schema check\nSHOW COLUMNS IN TABLE FIL.STORE;"
        queries = parse_sql_queries(sql)
        assert len(queries) == 1

    def test_query_name_extraction(self):
        sql = "-- HASH_AGG fingerprint for vw_store_summary\nSELECT HASH_AGG(*) FROM vz_apps.vw_store_summary;"
        queries = parse_sql_queries(sql)
        assert "HASH_AGG fingerprint" in queries[0]["name"]

    def test_default_category(self):
        sql = "SELECT 1;"
        queries = parse_sql_queries(sql)
        assert queries[0]["category"] == 1  # defaults to 1


class TestRunQueries:
    """Test run_queries graceful behavior without Snowflake credentials."""

    def test_no_credentials_returns_skip(self, monkeypatch):
        """Without credentials, should return SKIP result."""
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
        monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

        results = run_queries("SELECT 1;")
        assert len(results) == 1
        assert results[0].status == "SKIP"
        assert "credentials" in results[0].detail.lower()

    def test_empty_sql_no_credentials(self, monkeypatch):
        """Empty SQL with no credentials should return SKIP."""
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
        monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

        results = run_queries("")
        assert len(results) == 1
        assert results[0].status == "SKIP"


class TestSnowflakeResult:
    """Test SnowflakeResult dataclass."""

    def test_fields(self):
        r = SnowflakeResult(name="test", status="PASS", detail="ok", category=1)
        assert r.name == "test"
        assert r.status == "PASS"
        assert r.detail == "ok"
        assert r.category == 1

    def test_skip_status(self):
        r = SnowflakeResult(name="conn", status="SKIP", detail="no creds", category=0)
        assert r.status == "SKIP"
