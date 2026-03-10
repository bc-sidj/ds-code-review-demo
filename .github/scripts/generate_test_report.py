#!/usr/bin/env python3
"""Generate dynamic test cases for a PR and post a test report.

Reads the PR diff, sends it to an AI model to generate targeted test cases
specific to the changes, then runs those tests and produces a Markdown report.

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
import textwrap

from openai import OpenAI

DIFF_PATH = pathlib.Path("/tmp/diff.txt")
CONTEXT_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CONTEXT.md"
MAX_DIFF_CHARS = 80_000

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o"

GENERATE_PROMPT = """\
You are a senior QA engineer for the BigCommerce Data Solutions (DS) team. \
You are reviewing a Pull Request diff and must generate executable test cases.

{project_context}

Analyze the diff below and generate TWO things:

## 1. Python pytest test file

Write a complete, executable pytest file that tests the SPECIFIC changes in this PR. \
The tests should do static analysis of the changed files (parse the source code, check for patterns). \
Do NOT import airflow, snowflake, or any external dependencies — only use Python standard library + pytest.

For Python (.py) file changes, generate tests that check:
- All DS standards (catchup, default_args, snake_case, no hardcoded creds, etc.)
- Edge cases (empty data handling, null checks, divide-by-zero guards)
- Code quality (no unused imports, no TODOs, no deprecated APIs)

For SQL (.sql) file changes, generate tests that check:
- Fully qualified object names
- No USE SCHEMA statements
- COMMENT with Jira ticket present
- WHERE clause on UPDATE/DELETE
- COALESCE on LEFT JOIN columns
- Divide-by-zero protection

Each test must have a clear docstring explaining what it validates. \
Use descriptive test names like `test_dag_has_catchup_false` or `test_view_has_jira_comment`.

IMPORTANT — File paths: The test file will run in a temp directory, NOT inside the repo. \
To find the repo root, ALWAYS use `os.environ["REPO_ROOT"]`. \
For example, to read a DAG file:
```python
import os
repo_root = os.environ["REPO_ROOT"]
dag_path = os.path.join(repo_root, "dags", "dag_store_metrics.py")
with open(dag_path) as f:
    source = f.read()
```
NEVER use __file__ to resolve paths. NEVER hardcode /tmp/ or any absolute path. \
ALWAYS use os.environ["REPO_ROOT"] as the base for all file paths.

## 2. SQL validation queries

For any SQL file changes, generate validation queries that a reviewer could run in Snowflake:
- Row count check
- NULL check on key columns
- Duplicate check
- Range/sanity check on numeric columns

Output your response as JSON with this exact structure:
{{
  "test_file": "<complete pytest file content as a string>",
  "sql_queries": "<SQL validation queries as a string, or empty string if no SQL changes>",
  "test_summary": [
    {{"name": "test_name", "checks": "what it validates", "severity": "CRITICAL/WARNING/SUGGESTION"}}
  ]
}}

Output ONLY valid JSON. No markdown fences, no commentary.

Here is the diff:

```diff
{diff_content}
```
"""


def get_api_key() -> str:
    for var in ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var)
        if key:
            return key
    print("No API key found.", file=sys.stderr)
    sys.exit(1)


def load_project_context() -> str:
    if CONTEXT_MD_PATH.exists():
        content = CONTEXT_MD_PATH.read_text().strip()
        return f"Project context and team standards:\n\n{content}"
    return ""


def generate_tests(diff_content: str) -> dict:
    api_key = get_api_key()
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


def build_report(
    test_result: dict,
    pytest_output: str,
    passed: int,
    failed: int,
    errors: int,
) -> str:
    total = passed + failed + errors
    if failed == 0 and errors == 0:
        status = "ALL TESTS PASSED"
        status_icon = "✅"
    elif failed > 0:
        status = "TESTS FAILED — ACTION REQUIRED"
        status_icon = "❌"
    else:
        status = "ERRORS DETECTED"
        status_icon = "⚠️"

    report = []
    report.append("## Test Report — AI-Generated Test Cases")
    report.append("")
    report.append(f"**Status:** {status_icon} {status}")
    report.append(f"**Results:** {passed} passed, {failed} failed, {errors} errors out of {total} tests")
    report.append("")

    report.append("### Test Cases")
    report.append("")
    report.append("| # | Test | Validates | Severity | Result |")
    report.append("|---|------|-----------|----------|--------|")

    for i, test in enumerate(test_result.get("test_summary", []), 1):
        name = test.get("name", "unknown")
        checks = test.get("checks", "")
        severity = test.get("severity", "INFO")

        if name in pytest_output:
            if f"{name} PASSED" in pytest_output:
                result_icon = "✅ PASS"
            elif f"{name} FAILED" in pytest_output:
                result_icon = "❌ FAIL"
            else:
                result_icon = "⚠️ ERROR"
        else:
            result_icon = "➖ N/A"

        severity_icon = {"CRITICAL": "❌", "WARNING": "⚠️", "SUGGESTION": "ℹ️"}.get(severity, "ℹ️")
        report.append(f"| {i} | `{name}` | {checks} | {severity_icon} {severity} | {result_icon} |")

    report.append("")

    if test_result.get("sql_queries"):
        report.append("### SQL Validation Queries")
        report.append("")
        report.append("Run these in Snowflake to verify the changes:")
        report.append("")
        report.append("```sql")
        report.append(test_result["sql_queries"].strip())
        report.append("```")
        report.append("")

    report.append("<details>")
    report.append("<summary>Full pytest output</summary>")
    report.append("")
    report.append("```")
    report.append(pytest_output.strip())
    report.append("```")
    report.append("")
    report.append("</details>")
    report.append("")
    report.append("> Tests generated dynamically by AI based on the PR diff. "
                  "These replace manual test case documentation.")

    return "\n".join(report)


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

    test_result = generate_tests(diff_content)

    test_code = test_result.get("test_file", "")
    if not test_code.strip():
        print("## Test Report\n\nAI could not generate test cases for this diff.")
        return

    pytest_output, passed, failed, errors = run_tests(test_code, repo_root)

    report = build_report(test_result, pytest_output, passed, failed, errors)
    print(report)


if __name__ == "__main__":
    main()
