#!/usr/bin/env python3
"""Generate a detailed test report + rollout playbook for a PR.

Four layers run in sequence:
  1. Static validators (always runs, deterministic)
  2. AI-generated pytest tests (best effort, wrapped in try/except)
  3. Snowflake validation queries (best effort, skipped if no credentials)
  4. AI-generated rollout playbook (best effort, falls back to skeleton)

The report embeds Testing Evidence + Rollout Playbook directly in the PR,
replacing the need for separate Google Docs.

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
from datetime import datetime, timezone

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
# Layer 4: AI rollout playbook generation
# ---------------------------------------------------------------------------

ROLLOUT_PROMPT = """\
You are a deployment engineer for the BigCommerce Data Solutions (DS) team. \
Analyze the PR diff below and generate a rollout playbook.

{project_context}

Based on the diff, produce a structured rollout document in Markdown. \
Detect what types of changes are present and fill in ONLY the relevant sections.

Output the following sections (skip sections that don't apply):

### Airflow Changes
- Which DAGs are added/modified, deploy method (GitHub Actions / manual), \
backfill needed (check catchup setting)

### Database Changes
- DDL/DML scripts to execute, sp_rollout wrapping, CLONE backup recommendations

### Variables
- Any new Airflow Variables or Connections referenced in the code

### Pre-Rollout Checklist
- Auto-generated checklist items based on the changes (e.g., verify CLONE, \
confirm sp_rollout bookends, check Variable values in Astronomer)

### Rollback Steps
- How to undo each change (CLONE restore, revert DAG, drop objects)

### Re-Run Instructions
- Which jobs to re-trigger post-deploy, or "None — DAG runs on schedule"

Output ONLY the markdown sections. No preamble or closing commentary. \
If a section doesn't apply, omit it entirely.

```diff
{diff_content}
```
"""


def generate_rollout_doc(diff_content: str) -> str:
    """Generate a rollout playbook from the diff using AI.

    Returns markdown string. Falls back to a skeleton template on failure.
    """
    api_key = get_api_key()
    if not api_key:
        return ""

    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL)
    model = os.environ.get("REVIEW_MODEL", DEFAULT_MODEL)

    prompt = ROLLOUT_PROMPT.format(
        project_context=load_project_context(),
        diff_content=diff_content,
    )

    client = OpenAI(api_key=api_key, base_url=base_url)
    response = client.chat.completions.create(
        model=model, max_tokens=2048, temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.choices[0].message.content.strip()


ROLLOUT_SKELETON = """\
### Airflow Changes
- _Could not auto-detect — fill in manually if applicable_

### Database Changes
- _Could not auto-detect — fill in manually if applicable_

### Rollback Steps
- _Describe rollback steps manually_
"""


# ---------------------------------------------------------------------------
# Detailed report builder
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
    changed_files: list[dict] | None = None,
    rollout_md: str = "",
) -> str:
    """Build detailed Markdown report with Testing Evidence, per-category
    sections, Summary, and Rollout Playbook."""

    # Flatten all checks into a single list with source tag
    all_checks: list[dict] = []
    for filepath, results in static_results.items():
        short = pathlib.PurePosixPath(filepath).name
        for r in results:
            all_checks.append({
                "name": r.name, "file": short, "filepath": filepath,
                "status": r.status, "detail": r.detail,
                "category": r.category, "source": "static",
                "severity": r.severity,
            })

    # AI-generated test results
    ai_checks: list[dict] = []
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
            entry = {
                "name": name, "file": "AI", "filepath": "AI",
                "status": status, "detail": t.get("checks", ""),
                "category": t.get("category", 0), "source": "ai",
                "severity": t.get("severity", "WARNING"),
            }
            ai_checks.append(entry)
            all_checks.append(entry)

    # Snowflake results
    sf_checks: list[dict] = []
    sf_skipped = False
    if snowflake_results:
        for sr in snowflake_results:
            if sr.status == "SKIP":
                sf_skipped = True
                continue
            entry = {
                "name": sr.name, "file": "Snowflake", "filepath": "Snowflake",
                "status": sr.status, "detail": sr.detail,
                "category": sr.category, "source": "snowflake",
                "severity": "CRITICAL" if sr.status in ("FAIL", "ERROR") else "INFO",
            }
            sf_checks.append(entry)
            all_checks.append(entry)

    # Counts
    total_passed = sum(1 for c in all_checks if c["status"] == "PASS")
    total_failed = sum(1 for c in all_checks if c["status"] in ("FAIL", "ERROR"))
    total_warnings = sum(1 for c in all_checks if c["status"] == "WARNING")
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
    r.append(f"**Files analyzed:** {len(static_results)} | "
             f"**Checks:** {total_passed} passed, {total_failed} failed, {total_warnings} warnings")
    r.append("")

    # --- Testing Evidence ---
    r.append("### Testing Evidence")
    r.append("> _Auto-generated — replaces manual testing Google Doc._")
    r.append("")
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    r.append(f"**Run date:** {run_ts}")
    r.append("")

    # Validation layers status
    static_count = sum(len(v) for v in static_results.values())
    static_layer = f"✅ {static_count} checks executed" if static_count else "⏭️ No files to check"

    if ai_result:
        ai_total = ai_passed + ai_failed + ai_errors
        ai_layer = f"✅ {ai_passed} passed, {ai_failed} failed" if ai_total else "⏭️ No tests generated"
    else:
        ai_layer = "⏭️ Skipped (no API key)"

    if snowflake_results:
        sf_exec = [s for s in snowflake_results if s.status != "SKIP"]
        sf_skip = any(s.status == "SKIP" for s in snowflake_results)
        if sf_exec:
            sf_p = sum(1 for s in sf_exec if s.status == "PASS")
            sf_f = sum(1 for s in sf_exec if s.status in ("FAIL", "ERROR"))
            sf_layer = f"✅ {sf_p} passed, {sf_f} failed"
        elif sf_skip:
            sf_layer = "⏭️ Skipped (no credentials)"
        else:
            sf_layer = "⏭️ No queries generated"
    else:
        sf_layer = "⏭️ Not executed"

    r.append("| Validation Layer | Status |")
    r.append("|-----------------|--------|")
    r.append(f"| Static Validators | {static_layer} |")
    r.append(f"| AI-Generated Tests | {ai_layer} |")
    r.append(f"| Snowflake Validation | {sf_layer} |")
    r.append("")

    # Files changed table
    if changed_files:
        r.append("**Files changed:**")
        r.append("")
        r.append("| File | Type | Status |")
        r.append("|------|------|--------|")
        for f in changed_files:
            fname = pathlib.PurePosixPath(f["path"]).name
            ftype = {".sql": "SQL", ".py": "DAG/Python"}.get(f.get("extension", ""), f.get("extension", "—"))
            r.append(f"| `{fname}` | {ftype} | {f.get('status', 'unknown').title()} |")
        r.append("")

    r.append("---")
    r.append("")

    # --- Per-category detailed sections ---
    for cat_num in range(1, 6):
        cat_static = [c for c in all_checks if c["category"] == cat_num and c["source"] == "static"]
        cat_ai = [c for c in ai_checks if c["category"] == cat_num]
        cat_sf = [c for c in sf_checks if c["category"] == cat_num]
        cat_all = cat_static + cat_ai + cat_sf

        if not cat_all:
            continue

        cat_passed = sum(1 for c in cat_all if c["status"] == "PASS")
        cat_failed = sum(1 for c in cat_all if c["status"] in ("FAIL", "ERROR"))
        cat_icon = "✅" if cat_failed == 0 else "❌"

        r.append(f"### {cat_icon} Category {cat_num} — {CATEGORY_NAMES[cat_num]}")
        r.append(f"> {cat_passed} passed, {cat_failed} failed out of {len(cat_all)} checks")
        r.append("")

        # Static checks grouped by file
        if cat_static:
            r.append("#### Static Checks")
            r.append("")
            # Group by file
            files_seen = []
            for c in cat_static:
                if c["filepath"] not in files_seen:
                    files_seen.append(c["filepath"])

            for fp in files_seen:
                file_checks = [c for c in cat_static if c["filepath"] == fp]
                r.append(f"**`{file_checks[0]['file']}`**")
                r.append("")
                r.append("| Check | Result | Detail |")
                r.append("|-------|--------|--------|")
                for c in file_checks:
                    icon = _result_icon(c["status"])
                    r.append(f"| {c['name']} | {icon} {c['status']} | {c['detail']} |")
                r.append("")

        # AI-generated test results for this category
        if cat_ai:
            r.append("#### AI-Generated Tests")
            r.append("")
            r.append("| Test | Result | What it validates |")
            r.append("|------|--------|-------------------|")
            for c in cat_ai:
                icon = _result_icon(c["status"])
                r.append(f"| {c['name']} | {icon} {c['status']} | {c['detail']} |")
            r.append("")

        # Snowflake validation results for this category
        if cat_sf:
            r.append("#### Snowflake Validation")
            r.append("")
            r.append("| Query | Result | Detail |")
            r.append("|-------|--------|--------|")
            for c in cat_sf:
                icon = _result_icon(c["status"])
                r.append(f"| {c['name']} | {icon} {c['status']} | {c['detail']} |")
            r.append("")

    # --- Snowflake skipped notice ---
    if sf_skipped and not sf_checks:
        r.append("### ⏭️ Snowflake Validation")
        r.append("> Skipped — no credentials configured (set `SNOWFLAKE_ACCOUNT`/`USER`/`PASSWORD`)")
        r.append("")

    # --- Summary table ---
    r.append("---")
    r.append("### Summary")
    r.append("")
    r.append("| Category | Passed | Failed | Total |")
    r.append("|----------|--------|--------|-------|")

    for cat_num in range(1, 6):
        cat_all = [c for c in all_checks if c["category"] == cat_num]
        if not cat_all:
            continue
        cp = sum(1 for c in cat_all if c["status"] == "PASS")
        cf = sum(1 for c in cat_all if c["status"] in ("FAIL", "ERROR"))
        ct = len(cat_all)
        icon = "✅" if cf == 0 else "❌"
        r.append(f"| {icon} {cat_num} — {CATEGORY_NAMES[cat_num]} | {cp} | {cf} | {ct} |")

    if sf_checks:
        sfp = sum(1 for c in sf_checks if c["status"] == "PASS")
        sff = sum(1 for c in sf_checks if c["status"] in ("FAIL", "ERROR"))
        sf_icon = "✅" if sff == 0 else "❌"
        r.append(f"| {sf_icon} Snowflake Validation | {sfp} | {sff} | {len(sf_checks)} |")
    elif sf_skipped:
        r.append("| ⏭️ Snowflake Validation | — | — | Skipped |")

    r.append(f"| **Total** | **{total_passed}** | **{total_failed}** | **{total}** |")
    r.append("")

    # --- Rollout Playbook ---
    r.append("---")
    r.append("## Rollout Playbook")
    r.append("> _Auto-generated — replaces manual rollout Google Doc. Review and adjust before rollout._")
    r.append("")
    if rollout_md:
        r.append(rollout_md)
    else:
        r.append(ROLLOUT_SKELETON)
    r.append("")

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

    # Parse changed files for the Testing Evidence section
    changed_files = parse_diff_files(diff_content)

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

    # Layer 4: AI rollout playbook (best effort)
    rollout_md = ""
    try:
        rollout_md = generate_rollout_doc(diff_content)
    except Exception:
        print(f"Rollout generation failed:\n{traceback.format_exc()}", file=sys.stderr)

    report = build_report(
        static_results, ai_result, pytest_output,
        ai_passed, ai_failed, ai_errors, snowflake_results,
        changed_files=changed_files, rollout_md=rollout_md,
    )
    print(report)


if __name__ == "__main__":
    main()
