#!/usr/bin/env python3
"""Automated PR code review using an OpenAI-compatible API.

Reads a git diff from /tmp/diff.txt, sends it to an AI model for review,
and writes the resulting Markdown review to stdout.

Works with any OpenAI-compatible provider (OpenRouter, OpenAI, Anthropic, etc.)
by configuring these environment variables:

  OPENAI_API_KEY      — API key (also checks OPENROUTER_API_KEY, ANTHROPIC_API_KEY)
  API_BASE_URL        — Base URL (defaults to https://api.openai.com/v1)
  REVIEW_MODEL        — Model name (defaults to gpt-4o)
"""

import os
import sys
import pathlib

from openai import OpenAI

DIFF_PATH = pathlib.Path("/tmp/diff.txt")
CONTEXT_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CONTEXT.md"
MAX_DIFF_CHARS = 120_000

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o"

REVIEW_PROMPT = """\
You are reviewing a Pull Request for the BigCommerce Data Solutions (DS) team's \
dw_airflow repository. This repo contains Airflow DAGs (Python) and Snowflake DDL/DML (SQL).

{project_context}

Review the git diff below and output a thorough first-pass code review in \
GitHub-flavored Markdown using this structure:

## Automated Code Review

### DS Workflow Compliance
| Check | Status | Notes |
|-------|--------|-------|
| Jira ticket in commit message (DS-XXXX or ANALYTICS-XXXX) | PASS/FAIL | Which ticket found, or "None found" |
| Testing doc reminder | PASS/REMINDER | "Airflow code changed — ensure testing is done" or "No Airflow changes" |
| Rollout doc reminder | PASS/REMINDER | "DDL/DML detected — rollout doc needed" or "No DDL/DML" |
| DDL-only simplified process | INFO/N/A | "Only SQL files changed — simplified PR process may apply" |

### Code Review Findings
For EACH changed file, create a section with findings using these severity levels:
- ❌ CRITICAL — must fix before merge (security, missing WHERE on DML, broken logic)
- ⚠️ WARNING — should address (missing error handling, unhandled edge cases)
- ℹ️ SUGGESTION — nice to have (readability, performance)
- ✅ PASS — explicitly call out things done well

**For Python / Airflow files (.py) check:**
- catchup explicitly set (prefer False unless backfill intended)
- default_args has owner, start_date, retries, retry_delay
- Task deps correct (>> chaining, no orphaned tasks, no circular deps)
- retries and on_failure_callback present
- No hardcoded credentials or environment-specific paths
- No unused/missing imports, snake_case naming, idempotent design
- Edge cases: empty datasets, NULL in key fields, date/timezone issues, large volume

**For SQL files (.sql) check:**
- All object refs fully qualified (e.g., FIL.STORE not just STORE)
- No USE SCHEMA statements
- Object COMMENT includes ticket: COMMENT = 'DS-XXXX ...'
- All UPDATE/DELETE have WHERE clause (CRITICAL if missing)
- No unnecessary SELECT *, NULL propagation in JOINs, divide-by-zero
- Field length: will new data fit in existing VARCHAR columns?
- Non-deterministic ordering in QUALIFY/ROW_NUMBER (ambiguity risk)

**For Rollout SQL files check:**
- sp_rollout('start', ...) and sp_rollout('end', ...) bookends present
- Backout/backup tables use CLONE with COMMENT including ticket + drop date
- Multi-step rollouts have ordered comments (--Step 1:, --Step 2:, etc.)
- Role switching returns to dev role at end (USE ROLE FUJI_DEV_OWNER)
- Stash pattern used for deprecated objects (move to FUJI_STASH before drop)

### Edge Cases Summary
List all detected edge cases with file and line references.

### Generated Test Cases (5 DS Testing Categories)

**Category 1 — Data Integrity:** For each .sql file: HASH_AGG(*) before/after, \
row count check, MINUS comparison for exact diffs, column-level HASH_AGG EXCLUDE.

**Category 2 — Schema & DDL Compliance:** Schema inspection via information_schema, \
COMMENT verification, sp_rollout wrapping check.

**Category 3 — Regression:** HASH_AGG(* EXCLUDE (changed_cols)) DEV vs PROD, \
cross-environment comparison queries, historical data preservation checks.

**Category 4 — Edge Cases:** For .py: empty input, NULL, date boundaries, \
upstream failure, divide-by-zero, ambiguity. For .sql: NULL check on key cols, \
duplicate check, range/sanity check.

**Category 5 — Business Logic & Downstream:** Downstream dependency query \
(security.table_usage_summary), spot-check with known values, aggregate validation \
for financial tables.

### Summary
| Metric | Count |
|--------|-------|
| Critical issues | X |
| Warnings | X |
| Suggestions | X |
| Checks passed | X |

**Verdict:** READY FOR HUMAN REVIEW / ADDRESS WARNINGS / CRITICAL ISSUES FOUND

> This is an automated first-pass review. A human reviewer must still approve \
this PR. Separation of duties applies for production rollout per DS SOX \
compliance requirements.

Here is the diff to review:

```diff
{diff_content}
```
"""


def get_api_key() -> str:
    for var in ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var)
        if key:
            return key
    print(
        "No API key found. Set OPENROUTER_API_KEY, OPENAI_API_KEY, "
        "or ANTHROPIC_API_KEY as an environment variable.",
        file=sys.stderr,
    )
    sys.exit(1)


def load_project_context() -> str:
    if CONTEXT_MD_PATH.exists():
        content = CONTEXT_MD_PATH.read_text().strip()
        return f"Here is the project context and team standards:\n\n{content}"
    return ""


def main() -> None:
    if not DIFF_PATH.exists():
        print("No diff file found at /tmp/diff.txt", file=sys.stderr)
        sys.exit(1)

    diff_content = DIFF_PATH.read_text()

    if not diff_content.strip():
        print("## Automated Code Review\n\nNo changes detected in this PR.")
        return

    if len(diff_content) > MAX_DIFF_CHARS:
        diff_content = (
            diff_content[:MAX_DIFF_CHARS]
            + "\n\n... [diff truncated due to size] ..."
        )

    project_context = load_project_context()

    prompt = REVIEW_PROMPT.format(
        project_context=project_context,
        diff_content=diff_content,
    )

    api_key = get_api_key()
    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL)
    model = os.environ.get("REVIEW_MODEL", DEFAULT_MODEL)

    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    print(response.choices[0].message.content)


if __name__ == "__main__":
    main()
