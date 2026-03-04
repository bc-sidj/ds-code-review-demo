# Claude Code Prompt — Copy and Paste This

Copy everything below the line into Claude Code (PyCharm or terminal) to set up the automated code review demo repo end-to-end.

---

## THE PROMPT (copy from here)

```
I need you to help me set up and test the ds-code-review-demo repo. This is a proof-of-concept for automated code review for the BigCommerce Data Solutions (DS) team. Here's the full context:

## What This Repo Does

Two layers of automated code review using Claude:
1. **Layer 1 (Local):** A `/code-review` slash command I run in PyCharm before creating a PR. It reviews my git diff and generates a structured report.
2. **Layer 2 (GitHub Action):** An automatic review that triggers on every PR using `anthropics/claude-code-action@v1`. Uses OAuth token (no API key needed).

## Repo Structure

The repo should already be set up with:
- `.claude/commands/code-review.md` — the slash command
- `.github/workflows/claude-code-review.yml` — the GitHub Action
- `CLAUDE.md` — project memory with DS team standards
- `dags/buggy/dag_store_metrics_buggy.py` — sample DAG with 17 intentional bugs
- `dags/clean/dag_store_metrics_clean.py` — fixed version
- `ddl/fuji/vz_apps/buggy/vw_store_summary_buggy.sql` — sample SQL with 13 intentional issues
- `ddl/fuji/vz_apps/clean/vw_store_summary_clean.sql` — fixed version
- `code_reviews/` — gitignored folder for local review reports
- `docs/team_proposal.md` — proposal to share with my team

## What I Need You To Do Now

### Step 1: Verify the repo is set up correctly
- Check that all the files above exist
- Confirm the `.gitignore` has `code_reviews/*.md`
- Confirm the `CLAUDE.md` is present

### Step 2: Initialize git and push to GitHub
- Initialize git if not already done: `git init && git add . && git commit -m "Initial commit: DS code review automation POC"`
- Create a GitHub repo called `ds-code-review-demo` under my personal account and push:
  `gh repo create ds-code-review-demo --public --source=. --push`

### Step 3: Set up GitHub Action authentication
- Run `claude setup-token` and tell me to copy the token
- Tell me to add it as a GitHub secret called `CLAUDE_CODE_OAUTH_TOKEN`
- Tell me to install the Claude GitHub App at https://github.com/apps/claude

### Step 4: Create a test branch with buggy code
```bash
git checkout -b feature/DS-9999-test-buggy-code
cp dags/buggy/dag_store_metrics_buggy.py dags/dag_store_metrics.py
cp ddl/fuji/vz_apps/buggy/vw_store_summary_buggy.sql ddl/fuji/vz_apps/vw_store_summary.sql
git add .
git commit -m "DS-9999 Add store metrics DAG and summary view"
```

### Step 5: Run /code-review locally
Execute the code review slash command against this branch. Review the diff compared to main and generate the full structured report. Save it to `code_reviews/code_review_feature-DS-9999-test-buggy-code.md`.

### Step 6: Tell me what to do next
After the local review is done, tell me to:
1. Push the branch and open a PR to test the GitHub Action
2. Then create a second branch with the clean files to see a passing review
3. Share the `docs/team_proposal.md` with my team

## DS Team Standards (for reference during review)

**Git/PR:**
- Jira ticket (DS-XXXX) required in at least one commit
- Testing doc + rollout doc required for DDL/DML changes
- Separation of duties for production rollout (SOX)

**Python/Airflow:**
- catchup=False, default_args with owner/start_date/retries/retry_delay
- on_failure_callback on critical tasks
- No hardcoded credentials (use Airflow Variables/Connections)
- snake_case for DAG/task IDs, no unused imports
- Idempotent DAGs

**SQL/Snowflake:**
- Fully qualified names (FIL.STORE not STORE)
- No USE SCHEMA
- Object COMMENT with ticket number
- WHERE clause on all UPDATE/DELETE
- sp_rollout('start',...) and sp_rollout('end',...) in rollout docs
- COALESCE for NULL-prone LEFT JOIN columns

**Edge cases to always check:**
- Empty datasets, NULLs, divide-by-zero, duplicate rows
- Date boundaries, timezone/DST issues
- Large volume scenarios
```

## END OF PROMPT
