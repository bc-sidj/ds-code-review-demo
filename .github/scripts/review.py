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
CLAUDE_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CLAUDE.md"
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

### Edge Cases Summary
List all detected edge cases with file and line references.

### Generated Test Cases

**Python (pytest stubs):** For each .py file, stubs for: happy path, empty input, \
NULL values, date boundaries, upstream failure.

**SQL (validation queries):** For each .sql file: row count check, NULL check on \
key columns, duplicate check, range/sanity check, spot-check.

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
    if CLAUDE_MD_PATH.exists():
        content = CLAUDE_MD_PATH.read_text().strip()
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
