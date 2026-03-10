#!/usr/bin/env python3
"""Generate a compact test report for a PR.

Three layers run in sequence:
  1. Static validators (always runs, deterministic)
  2. AI-generated pytest tests (best effort, wrapped in try/except)
  3. Snowflake validation queries (best effort, skipped if no credentials)

The report is compact: summary table + only failing categories expanded.

Environment variables:
  OPENAI_API_KEY / OPENROUTER_API_KEY / ANTHROPIC_API_KEY
  API_BASE_URL        — defaults to https://api.openai.com/v1
  REVIEW_MODEL        — defaults to gpt-4o
  SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD / etc.
"""

import os
import sys
import json
import pathlib
import subprocess
import tempfile
import traceback

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from validators import (
    CheckResult,
    CATEGORY_NAMES,
    validate_sql_file,
    validate_dag_file,
    parse_diff_files,
)
from snowflake_runner import SnowflakeResult, run_queries as run_snowflake_queries

from openai import OpenAI

DIFF_PATH = pathlib.Path("/tmp/diff.txt")
CONTEXT_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CONTEXT.md"
MAX_DIFF_CHARS = 80_000

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o"

# ---------------------------------------------------------------------------
# AI prompt
# ---------------------------------------------------------------------------

GENERATE_PROMPT = """\
You are a senior QA engineer for the BigCommerce Data Solutions (DS) team. \
You are reviewing a Pull Request diff and must generate executable test cases \
organized by the 5 DS Testing Categories.

{project_context}

Analyze the diff below and generate TWO things:

## 1. Python pytest test file

Write a complete, executable pytest file that tests the SPECIFIC changes in this PR. \
Static analysis only — no airflow/snowflake imports. Use Python stdlib + pytest.

Organize by category:
- TestCategory1DataIntegrity, TestCategory2SchemaDDL, TestCategory3Regression, \
TestCategory4EdgeCases, TestCategory5BusinessLogic

Use `os.environ["REPO_ROOT"]` for file paths. Never use __file__.

## 2. SQL validation queries

For SQL changes, generate Snowflake queries per category:
- Cat 1: HASH_AGG(*) before/after, COUNT(*), MINUS comparison
- Cat 2: information_schema queries, COMMENT verification
- Cat 3: HASH_AGG(* EXCLUDE (changed_cols)) DEV vs PROD
- Cat 4: NULL checks, duplicate checks, range/sanity checks
- Cat 5: security.table_usage_summary, spot-checks

Format each query with a comment name and category header:
```
-- Category 1
-- Row count check for vw_store_summary
SELECT COUNT(*) FROM ...;
```

Output JSON only:
{{
  "test_file": "<pytest file content>",
  "sql_queries": "<SQL queries or empty string>",
  "test_summary": [
    {{"name": "test_name", "checks": "what it validates", "severity": "CRITICAL/WARNING/SUGGESTION", "category": 1}}
  ]
}}

```diff
{diff_content}
```
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    for var in ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var)
        if key:
            return key
    return ""


def load_project_context() -> str:
    if CONTEXT_MD_PATH.exists():
        content = CONTEXT_MD_PATH.read_text().strip()
        return f"Project context:\n\n{content}"
    return ""


# ---------------------------------------------------------------------------
# Layer 1: Static validators
# ---------------------------------------------------------------------------

def run_static_validators(diff_text: str, repo_root: str) -> dict[str, list[CheckResult]]:
    """Run validators on each changed file. Returns {filepath: [results]}."""
    changed_files = parse_diff_files(diff_text)
    all_results: dict[str, list[CheckResult]] = {}

    for f in changed_files:
        path, ext, status = f["path"], f["extension"], f["status"]
        if status in ("deleted", "binary"):
            continue

        full_path = pathlib.Path(repo_root) / path
        if not full_path.exists():
            continue

        content = full_path.read_text()
        if ext == ".sql":
            results = validate_sql_file(content, path)
        elif ext == ".py":
            results = validate_dag_file(content, path)
        else:
            continue

        if results:
            all_results[path] = results

    return all_results


# ---------------------------------------------------------------------------
# Layer 2: AI test generation + execution
# ---------------------------------------------------------------------------

def generate_tests(diff_content: str) -> dict:
    api_key = get_api_key()
    if not api_key:
        return {}

    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL)
    model = os.environ.get("REVIEW_MODEL", DEFAULT_MODEL)

    prompt = GENERATE_PROMPT.format(
        project_context=load_project_context(),
        diff_content=diff_content,
    )

    client = OpenAI(api_key=api_key, base_url=base_url)
    response = client.chat.completions.create(
        model=model, max_tokens=4096, temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:-3]

    return json.loads(raw)


def run_tests(test_code: str, repo_root: str) -> tuple[str, int, int, int]:
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = pathlib.Path(tmpdir) / "test_pr_changes.py"
        test_file.write_text(test_code)

        env = os.environ.copy()
        env["REPO_ROOT"] = repo_root

        result = subprocess.run(
            [sys.executable, "-m", "pytest", str(test_file), "-v", "--tb=short",
             f"--rootdir={repo_root}"],
            capture_output=True, text=True, cwd=repo_root, env=env,
        )

        output = result.stdout + result.stderr
        return output, output.count(" PASSED"), output.count(" FAILED"), output.count(" ERROR")


# ---------------------------------------------------------------------------
# Compact report builder
# ---------------------------------------------------------------------------

def _result_icon(status: str) -> str:
    return {"PASS": "✅", "FAIL": "❌", "WARNING": "⚠️", "SKIP": "⏭️", "ERROR": "💥"}.get(status, "❓")


def build_report(
    static_results: dict[str, list[CheckResult]],
    ai_result: dict | None,
    pytest_output: str,
    ai_passed: int,
    ai_failed: int,
    ai_errors: int,
    snowflake_results: list[SnowflakeResult] | None,
) -> str:
    """Build compact Markdown report: summary table + failing categories expanded."""

    # Flatten all checks into a single list with source tag
    all_checks: list[dict] = []
    for filepath, results in static_results.items():
        short = pathlib.PurePosixPath(filepath).name
        for r in results:
            all_checks.append({
                "name": r.name, "file": short, "status": r.status,
                "detail": r.detail, "category": r.category, "source": "static",
            })

    # AI-generated test results
    if ai_result:
        for t in ai_result.get("test_summary", []):
            name = t.get("name", "unknown")
            if pytest_output and name in pytest_output:
                if f"{name} PASSED" in pytest_output:
                    status = "PASS"
                elif f"{name} FAILED" in pytest_output:
                    status = "FAIL"
                else:
                    status = "ERROR"
            else:
                status = "PASS" if ai_passed > 0 else "SKIP"
            all_checks.append({
                "name": name, "file": "AI", "status": status,
                "detail": t.get("checks", ""), "category": t.get("category", 0),
                "source": "ai",
            })

    # Snowflake results
    sf_passed = sf_failed = 0
    sf_skipped = False
    if snowflake_results:
        for sr in snowflake_results:
            if sr.status == "SKIP":
                sf_skipped = True
                continue
            all_checks.append({
                "name": sr.name, "file": "Snowflake", "status": sr.status,
                "detail": sr.detail, "category": sr.category, "source": "snowflake",
            })
            if sr.status == "PASS":
                sf_passed += 1
            elif sr.status in ("FAIL", "ERROR"):
                sf_failed += 1

    # Counts
    total_passed = sum(1 for c in all_checks if c["status"] == "PASS")
    total_failed = sum(1 for c in all_checks if c["status"] == "FAIL")
    total = len(all_checks)

    if total_failed == 0:
        status_text = "ALL CHECKS PASSED"
        status_icon = "✅"
    else:
        status_text = "ISSUES FOUND"
        status_icon = "❌"

    r = []
    r.append("## Test Report")
    r.append("")
    r.append(f"**Status:** {status_icon} {status_text}")
    r.append(f"**Files:** {len(static_results)} | **Checks:** {total_passed} passed, {total_failed} failed")
    r.append("")

    # --- Summary table ---
    r.append("| Category | Result |")
    r.append("|----------|--------|")

    failing_cats = []
    for cat_num in range(1, 6):
        cat_checks = [c for c in all_checks if c["category"] == cat_num]
        cp = sum(1 for c in cat_checks if c["status"] == "PASS")
        cf = sum(1 for c in cat_checks if c["status"] in ("FAIL", "ERROR"))
        ct = cp + cf + sum(1 for c in cat_checks if c["status"] in ("WARNING", "SKIP"))
        icon = "✅" if cf == 0 else "❌"
        r.append(f"| {cat_num} — {CATEGORY_NAMES[cat_num]} | {icon} {cp}/{ct} |")
        if cf > 0:
            failing_cats.append(cat_num)

    # Snowflake row
    if snowflake_results:
        sf_checks = [c for c in all_checks if c["source"] == "snowflake"]
        if sf_skipped:
            r.append("| Snowflake Validation | ⏭️ Skipped (no credentials) |")
        elif sf_checks:
            sf_total = len(sf_checks)
            sf_p = sum(1 for c in sf_checks if c["status"] == "PASS")
            sf_icon = "✅" if sf_failed == 0 else "❌"
            r.append(f"| Snowflake Validation | {sf_icon} {sf_p}/{sf_total} |")

    r.append("")

    # --- Expand failing categories ---
    for cat_num in failing_cats:
        cat_checks = [c for c in all_checks if c["category"] == cat_num and c["status"] in ("FAIL", "ERROR")]
        r.append(f"### ❌ Category {cat_num} — {CATEGORY_NAMES[cat_num]}")
        r.append("")
        r.append("| Check | File | Detail |")
        r.append("|-------|------|--------|")
        for c in cat_checks:
            r.append(f"| {c['name']} | {c['file']} | ❌ {c['detail']} |")
        r.append("")

    # --- All checks collapsed ---
    r.append(f"<details><summary>All checks ({total})</summary>")
    r.append("")
    r.append("| Check | File | Cat | Result |")
    r.append("|-------|------|-----|--------|")
    for c in all_checks:
        icon = _result_icon(c["status"])
        cat_label = f"{c['category']}" if c["category"] in range(1, 6) else "—"
        r.append(f"| {c['name']} | {c['file']} | {cat_label} | {icon} {c['status']} |")
    r.append("")
    r.append("</details>")

    return "\n".join(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DIFF_PATH.exists():
        print("No diff file found at /tmp/diff.txt", file=sys.stderr)
        sys.exit(1)

    diff_content = DIFF_PATH.read_text()
    if not diff_content.strip():
        print("## Test Report\n\nNo changes detected.")
        return

    if len(diff_content) > MAX_DIFF_CHARS:
        diff_content = diff_content[:MAX_DIFF_CHARS] + "\n\n... [truncated] ..."

    repo_root = os.environ.get("GITHUB_WORKSPACE", str(pathlib.Path.cwd()))

    # Layer 1: Static validators (always runs)
    static_results = run_static_validators(diff_content, repo_root)

    # Layer 2: AI-generated tests (best effort)
    ai_result = None
    pytest_output = ""
    ai_passed = ai_failed = ai_errors = 0
    try:
        ai_result = generate_tests(diff_content)
        test_code = ai_result.get("test_file", "")
        if test_code.strip():
            pytest_output, ai_passed, ai_failed, ai_errors = run_tests(test_code, repo_root)
    except Exception:
        print(f"AI layer failed:\n{traceback.format_exc()}", file=sys.stderr)

    # Layer 3: Snowflake validation (best effort)
    snowflake_results = None
    try:
        sql_queries = ai_result.get("sql_queries", "") if ai_result else ""
        if sql_queries:
            snowflake_results = run_snowflake_queries(sql_queries)
    except Exception:
        print(f"Snowflake layer failed:\n{traceback.format_exc()}", file=sys.stderr)

    report = build_report(
        static_results, ai_result, pytest_output,
        ai_passed, ai_failed, ai_errors, snowflake_results,
    )
    print(report)


if __name__ == "__main__":
    main()
