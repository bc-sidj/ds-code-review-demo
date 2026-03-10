#!/usr/bin/env python3
"""Generate a test report for a PR by executing static validators and AI tests.

Reads the PR diff, runs deterministic validators on each changed file (organized
by 5 DS Testing Categories), then sends the diff to an AI model for additional
contextual test cases. Both layers are combined into a single Markdown report.

The static validators always run. The AI layer is wrapped in try/except so the
report still posts even if the API call fails.

Environment variables:
  OPENAI_API_KEY      — API key (also checks OPENROUTER_API_KEY, ANTHROPIC_API_KEY)
  API_BASE_URL        — Base URL (defaults to https://api.openai.com/v1)
  REVIEW_MODEL        — Model name (defaults to gpt-4o)
"""

import os
import sys
import json
import pathlib
import subprocess
import tempfile
import traceback

# Ensure the scripts directory is on the path so we can import validators
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from validators import (
    CheckResult,
    CATEGORY_NAMES,
    validate_sql_file,
    validate_dag_file,
    parse_diff_files,
)

from openai import OpenAI

DIFF_PATH = pathlib.Path("/tmp/diff.txt")
CONTEXT_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CONTEXT.md"
MAX_DIFF_CHARS = 80_000

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o"

# ---------------------------------------------------------------------------
# AI prompt — generates pytest tests organized by 5 DS Testing Categories
# ---------------------------------------------------------------------------

GENERATE_PROMPT = """\
You are a senior QA engineer for the BigCommerce Data Solutions (DS) team. \
You are reviewing a Pull Request diff and must generate executable test cases \
organized by the 5 DS Testing Categories.

{project_context}

Analyze the diff below and generate TWO things:

## 1. Python pytest test file

Write a complete, executable pytest file that tests the SPECIFIC changes in this PR. \
The tests should do static analysis of the changed files (parse the source code, check for patterns). \
Do NOT import airflow, snowflake, or any external dependencies — only use Python standard library + pytest.

Organize tests by DS Testing Category using class grouping:

```python
class TestCategory1DataIntegrity:
    def test_explicit_column_list(self): ...
    def test_date_filtering_present(self): ...

class TestCategory2SchemaDDL:
    def test_comment_with_ticket(self): ...

class TestCategory3Regression:
    def test_update_has_where(self): ...

class TestCategory4EdgeCases:
    def test_divide_by_zero_guard(self): ...
    def test_null_handling(self): ...

class TestCategory5BusinessLogic:
    def test_downstream_documented(self): ...
```

For Python (.py) file changes, check:
- catchup set, default_args complete, on_failure_callback, no hardcoded creds
- Edge cases: empty data, null checks, divide-by-zero
- No unused imports, no TODOs, no deprecated APIs

For SQL (.sql) file changes, check:
- Fully qualified names, no USE SCHEMA, COMMENT with ticket, WHERE on DML
- COALESCE on LEFT JOIN, divide-by-zero protection, ROW_NUMBER determinism

IMPORTANT — File paths: Use `os.environ["REPO_ROOT"]` for all paths. \
NEVER use __file__ or hardcode absolute paths.

## 2. SQL validation queries (organized by 5 DS Testing Categories)

For SQL changes, generate Snowflake validation queries per category:

**Category 1 — Data Integrity:** HASH_AGG(*) before/after, COUNT(*), MINUS comparison
**Category 2 — Schema & DDL Compliance:** information_schema queries, COMMENT verification
**Category 3 — Regression:** HASH_AGG(* EXCLUDE (changed_cols)) DEV vs PROD
**Category 4 — Edge Cases:** NULL checks, duplicate checks, range/sanity checks
**Category 5 — Business Logic & Downstream:** security.table_usage_summary, spot-checks

Output your response as JSON with this exact structure:
{{
  "test_file": "<complete pytest file content as a string>",
  "sql_queries": "<SQL validation queries organized by category, or empty string>",
  "test_summary": [
    {{"name": "test_name", "checks": "what it validates", "severity": "CRITICAL/WARNING/SUGGESTION", "category": 1}}
  ]
}}

Output ONLY valid JSON. No markdown fences, no commentary.

Here is the diff:

```diff
{diff_content}
```
"""


# ---------------------------------------------------------------------------
# API helpers
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
        return f"Project context and team standards:\n\n{content}"
    return ""


# ---------------------------------------------------------------------------
# Static validation — runs validators on each changed file
# ---------------------------------------------------------------------------

def run_static_validators(diff_text: str, repo_root: str) -> dict[str, list[CheckResult]]:
    """Parse diff, read changed files, run validators. Returns {filepath: [results]}."""
    changed_files = parse_diff_files(diff_text)
    all_results: dict[str, list[CheckResult]] = {}

    for f in changed_files:
        path = f["path"]
        ext = f["extension"]
        status = f["status"]

        # Skip deleted or binary files
        if status in ("deleted", "binary"):
            continue

        full_path = pathlib.Path(repo_root) / path
        if not full_path.exists():
            continue

        content = full_path.read_text()

        if ext == ".sql":
            results = validate_sql_file(content, path)
        elif ext == ".py" and ("dag" in path.lower() or "dags/" in path):
            results = validate_dag_file(content, path)
        elif ext == ".py":
            # Run a subset of DAG validators for non-DAG Python files
            results = validate_dag_file(content, path)
        else:
            continue

        if results:
            all_results[path] = results

    return all_results


# ---------------------------------------------------------------------------
# AI test generation + execution
# ---------------------------------------------------------------------------

def generate_tests(diff_content: str) -> dict:
    """Call AI to generate pytest tests and SQL queries."""
    api_key = get_api_key()
    if not api_key:
        return {}

    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL)
    model = os.environ.get("REVIEW_MODEL", DEFAULT_MODEL)
    project_context = load_project_context()

    prompt = GENERATE_PROMPT.format(
        project_context=project_context,
        diff_content=diff_content,
    )

    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model=model,
        max_tokens=4096,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.choices[0].message.content.strip()

    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:-3]

    return json.loads(raw)


def run_tests(test_code: str, repo_root: str) -> tuple[str, int, int, int]:
    """Run AI-generated pytest file and return output + counts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = pathlib.Path(tmpdir) / "test_pr_changes.py"
        test_file.write_text(test_code)

        env = os.environ.copy()
        env["REPO_ROOT"] = repo_root

        result = subprocess.run(
            [
                sys.executable, "-m", "pytest",
                str(test_file),
                "-v",
                "--tb=short",
                f"--rootdir={repo_root}",
            ],
            capture_output=True,
            text=True,
            cwd=repo_root,
            env=env,
        )

        output = result.stdout + result.stderr

        passed = output.count(" PASSED")
        failed = output.count(" FAILED")
        errors = output.count(" ERROR")

        return output, passed, failed, errors


# ---------------------------------------------------------------------------
# Report builder — organized by 5 DS Testing Categories
# ---------------------------------------------------------------------------

def build_report(
    static_results: dict[str, list[CheckResult]],
    ai_result: dict | None,
    pytest_output: str,
    ai_passed: int,
    ai_failed: int,
    ai_errors: int,
) -> str:
    """Build the full Markdown report organized by 5 DS Testing Categories."""

    # Flatten static results for counting
    all_static: list[CheckResult] = []
    for results in static_results.values():
        all_static.extend(results)

    static_passed = sum(1 for r in all_static if r.status == "PASS")
    static_failed = sum(1 for r in all_static if r.status == "FAIL")
    static_warned = sum(1 for r in all_static if r.status == "WARNING")

    total_checks = len(all_static) + ai_passed + ai_failed + ai_errors
    total_failed = static_failed + ai_failed
    total_passed = static_passed + ai_passed

    if total_failed == 0 and ai_errors == 0:
        status = "ALL CHECKS PASSED"
        status_icon = "✅"
    elif total_failed > 0:
        status = "ISSUES FOUND"
        status_icon = "❌"
    else:
        status = "WARNINGS DETECTED"
        status_icon = "⚠️"

    report = []
    report.append("## Test Report — Automated Validation Results")
    report.append("")
    report.append(f"**Status:** {status_icon} {status}")
    report.append(
        f"**Files validated:** {len(static_results)} | "
        f"**Checks executed:** {total_passed} passed, {total_failed} failed, "
        f"{static_warned} warnings"
    )
    report.append("")
    report.append("---")
    report.append("")

    # -----------------------------------------------------------------------
    # Static results organized by category
    # -----------------------------------------------------------------------
    for cat_num in range(1, 6):
        cat_name = CATEGORY_NAMES[cat_num]
        report.append(f"### Category {cat_num} — {cat_name}")
        report.append("")
        report.append("#### Static Checks (Executed)")
        report.append("")
        report.append("| File | Check | Result | Detail |")
        report.append("|------|-------|--------|--------|")

        has_rows = False
        for filepath, results in static_results.items():
            short_name = pathlib.PurePosixPath(filepath).name
            for r in results:
                if r.category != cat_num:
                    continue
                has_rows = True
                if r.status == "PASS":
                    icon = "✅ PASS"
                elif r.status == "FAIL":
                    icon = "❌ FAIL"
                else:
                    icon = "⚠️ WARNING"
                report.append(f"| {short_name} | {r.name} | {icon} | {r.detail} |")

        if not has_rows:
            report.append("| — | No checks applicable | — | — |")

        # Add AI-generated tests for this category
        if ai_result:
            cat_ai_tests = [
                t for t in ai_result.get("test_summary", [])
                if t.get("category") == cat_num
            ]
            if cat_ai_tests:
                report.append("")
                report.append("#### AI-Generated Tests")
                report.append("")
                report.append("| Test | Validates | Severity | Result |")
                report.append("|------|-----------|----------|--------|")
                for test in cat_ai_tests:
                    name = test.get("name", "unknown")
                    checks = test.get("checks", "")
                    severity = test.get("severity", "INFO")
                    if pytest_output and name in pytest_output:
                        if f"{name} PASSED" in pytest_output:
                            result_icon = "✅ PASS"
                        elif f"{name} FAILED" in pytest_output:
                            result_icon = "❌ FAIL"
                        else:
                            result_icon = "⚠️ ERROR"
                    else:
                        result_icon = "➖ N/A"
                    sev_icon = {"CRITICAL": "❌", "WARNING": "⚠️", "SUGGESTION": "ℹ️"}.get(severity, "ℹ️")
                    report.append(f"| `{name}` | {checks} | {sev_icon} {severity} | {result_icon} |")

        # Add Snowflake queries for this category (from AI)
        if ai_result and ai_result.get("sql_queries"):
            # Extract category-specific SQL block if present
            sql_text = ai_result["sql_queries"]
            cat_header = f"Category {cat_num}"
            if cat_header in sql_text:
                # Try to extract just this category's queries
                start = sql_text.index(cat_header)
                # Find next category or end
                next_cat = None
                for nc in range(cat_num + 1, 6):
                    marker = f"Category {nc}"
                    if marker in sql_text[start + len(cat_header):]:
                        next_cat = sql_text.index(marker, start + len(cat_header))
                        break
                cat_sql = sql_text[start:next_cat].strip() if next_cat else sql_text[start:].strip()
                if cat_sql:
                    report.append("")
                    report.append("#### Snowflake Validation (Requires Manual Run)")
                    report.append("")
                    report.append("```sql")
                    report.append(cat_sql)
                    report.append("```")

        report.append("")
        report.append("---")
        report.append("")

    # -----------------------------------------------------------------------
    # Summary table
    # -----------------------------------------------------------------------
    report.append("### Summary")
    report.append("")
    report.append("| Category | Passed | Failed | Warnings |")
    report.append("|----------|--------|--------|----------|")

    grand_passed = 0
    grand_failed = 0
    grand_warned = 0
    for cat_num in range(1, 6):
        cat_name = CATEGORY_NAMES[cat_num]
        cat_checks = [r for r in all_static if r.category == cat_num]
        cp = sum(1 for r in cat_checks if r.status == "PASS")
        cf = sum(1 for r in cat_checks if r.status == "FAIL")
        cw = sum(1 for r in cat_checks if r.status == "WARNING")
        grand_passed += cp
        grand_failed += cf
        grand_warned += cw
        report.append(f"| {cat_num} — {cat_name} | {cp} | {cf} | {cw} |")

    report.append(f"| **Total (Static)** | **{grand_passed}** | **{grand_failed}** | **{grand_warned}** |")

    if ai_result:
        report.append(f"| AI-Generated Tests | {ai_passed} | {ai_failed} | {ai_errors} |")

    report.append("")

    # -----------------------------------------------------------------------
    # Pytest output (collapsed)
    # -----------------------------------------------------------------------
    if pytest_output:
        report.append("<details>")
        report.append("<summary>Full pytest output</summary>")
        report.append("")
        report.append("```")
        report.append(pytest_output.strip())
        report.append("```")
        report.append("")
        report.append("</details>")
        report.append("")

    report.append("> Static checks executed automatically. AI-generated tests "
                  "provide additional contextual coverage. Snowflake queries "
                  "require manual execution in the target environment.")

    return "\n".join(report)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DIFF_PATH.exists():
        print("No diff file found at /tmp/diff.txt", file=sys.stderr)
        sys.exit(1)

    diff_content = DIFF_PATH.read_text()

    if not diff_content.strip():
        print("## Test Report\n\nNo changes detected — no tests to run.")
        return

    if len(diff_content) > MAX_DIFF_CHARS:
        diff_content = (
            diff_content[:MAX_DIFF_CHARS]
            + "\n\n... [diff truncated due to size] ..."
        )

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
            pytest_output, ai_passed, ai_failed, ai_errors = run_tests(
                test_code, repo_root
            )
    except Exception:
        print(
            f"AI test generation failed (static results still available):\n"
            f"{traceback.format_exc()}",
            file=sys.stderr,
        )

    report = build_report(
        static_results, ai_result, pytest_output,
        ai_passed, ai_failed, ai_errors,
    )
    print(report)


if __name__ == "__main__":
    main()
