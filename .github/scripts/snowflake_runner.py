"""Execute Snowflake validation queries and return CheckResult objects.

Connects using environment variables. Returns empty results gracefully
when credentials are not configured.

Environment variables:
  SNOWFLAKE_ACCOUNT    — e.g. bc12345.us-east-1
  SNOWFLAKE_USER       — service account
  SNOWFLAKE_PASSWORD   — password (or use key-pair auth)
  SNOWFLAKE_WAREHOUSE  — compute warehouse
  SNOWFLAKE_DATABASE   — default database (e.g. FUJI)
  SNOWFLAKE_ROLE       — role to use (e.g. FUJI_DEV_OWNER)
"""

import os
import re
import sys
from dataclasses import dataclass
from typing import Literal


@dataclass
class SnowflakeResult:
    """Result of a single Snowflake validation query."""
    name: str
    status: Literal["PASS", "FAIL", "SKIP", "ERROR"]
    detail: str
    category: int  # 1-5


def _get_connection_params() -> dict | None:
    """Return Snowflake connection params from env vars, or None if missing."""
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    user = os.environ.get("SNOWFLAKE_USER", "")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    if not (account and user and password):
        return None
    return {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "FUJI"),
        "role": os.environ.get("SNOWFLAKE_ROLE", ""),
    }


def parse_sql_queries(sql_text: str) -> list[dict]:
    """Parse AI-generated SQL text into individual executable queries.

    Returns list of {sql, name, category} dicts.
    """
    if not sql_text or not sql_text.strip():
        return []

    queries = []
    current_category = 0
    current_name = ""

    for line in sql_text.splitlines():
        stripped = line.strip()

        # Detect category headers
        cat_match = re.match(r'(?:--|##?\s*)?\s*(?:\*\*)?Category\s+(\d)', stripped, re.IGNORECASE)
        if cat_match:
            current_category = int(cat_match.group(1))
            continue

        # Detect query names from comments
        if stripped.startswith("--") and not stripped.startswith("---"):
            name_text = stripped.lstrip("- ").strip()
            if name_text and len(name_text) > 3:
                current_name = name_text[:80]
            continue

        # Skip empty lines and markdown
        if not stripped or stripped.startswith("```") or stripped.startswith("#"):
            continue

        # Accumulate SQL statement
        if any(kw in stripped.upper() for kw in ("SELECT", "SHOW", "DESCRIBE", "CALL")):
            # Collect until semicolon
            stmt = stripped
            if not stmt.endswith(";"):
                stmt += ";"

            queries.append({
                "sql": stmt.rstrip(";"),
                "name": current_name or f"Query {len(queries) + 1}",
                "category": current_category if current_category in range(1, 6) else 1,
            })
            current_name = ""

    return queries


def run_queries(sql_text: str) -> list[SnowflakeResult]:
    """Execute SQL queries against Snowflake. Returns results.

    Gracefully returns empty list when credentials are missing.
    """
    params = _get_connection_params()
    if not params:
        return [SnowflakeResult(
            name="Snowflake connection",
            status="SKIP",
            detail="No credentials configured — set SNOWFLAKE_ACCOUNT/USER/PASSWORD",
            category=0,
        )]

    queries = parse_sql_queries(sql_text)
    if not queries:
        return []

    results = []

    try:
        import snowflake.connector
    except ImportError:
        return [SnowflakeResult(
            name="Snowflake connector",
            status="SKIP",
            detail="snowflake-connector-python not installed",
            category=0,
        )]

    conn = None
    try:
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()

        for q in queries:
            try:
                cursor.execute(q["sql"])
                rows = cursor.fetchall()
                row_count = len(rows)

                # Determine pass/fail based on query intent
                detail = f"{row_count} row(s) returned"
                if rows and len(rows[0]) > 0:
                    # Show first row preview (truncated)
                    preview = str(rows[0])[:100]
                    detail += f" | {preview}"

                # Heuristic: queries checking for issues (NULL, duplicates) FAIL if rows > 0
                name_upper = q["name"].upper()
                if any(kw in name_upper for kw in ("NULL CHECK", "DUPLICATE", "ORPHAN")):
                    status = "FAIL" if row_count > 0 else "PASS"
                elif "COUNT" in name_upper or "HASH" in name_upper:
                    status = "PASS" if row_count > 0 else "FAIL"
                else:
                    status = "PASS"

                results.append(SnowflakeResult(
                    name=q["name"],
                    status=status,
                    detail=detail,
                    category=q["category"],
                ))
            except Exception as e:
                results.append(SnowflakeResult(
                    name=q["name"],
                    status="ERROR",
                    detail=str(e)[:120],
                    category=q["category"],
                ))

    except Exception as e:
        results.append(SnowflakeResult(
            name="Snowflake connection",
            status="ERROR",
            detail=f"Connection failed: {str(e)[:100]}",
            category=0,
        ))
    finally:
        if conn:
            conn.close()

    return results
