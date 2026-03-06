# Cursor Setup Prompt — Copy and Paste This

Copy the prompt below into Cursor's AI chat to set up and test the automated code review demo repo end-to-end.

---

## THE PROMPT (copy from here)

```
I need you to help me set up and test the ds-code-review-demo repo. This is a proof-of-concept for automated code review for the BigCommerce Data Solutions (DS) team. Here's the full context:

## What This Repo Does

Two layers of automated code review:
1. **Layer 1 (Local):** I ask you to follow the review instructions in `.claude/commands/code-review.md`. You review my git diff and generate a structured report.
2. **Layer 2 (GitHub Action):** An automatic review that triggers on every PR. The workflow calls the Anthropic API to review the diff and posts a comment on the PR.

## Repo Structure

The repo should already be set up with:
- `.claude/commands/code-review.md` — the review instructions
- `.github/workflows/claude-code-review.yml` — the GitHub Action
- `.github/scripts/review.py` — the Python script that calls the Anthropic API
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
- Tell me to sign up at openrouter.ai, create an API key, and add it as a GitHub secret called `OPENROUTER_API_KEY`
  (Repo Settings → Secrets and variables → Actions → New repository secret)

### Step 4: Create a test branch with buggy code
```bash
git checkout -b feature/DS-9999-test-buggy-code
cp dags/buggy/dag_store_metrics_buggy.py dags/dag_store_metrics.py
cp ddl/fuji/vz_apps/buggy/vw_store_summary_buggy.sql ddl/fuji/vz_apps/vw_store_summary.sql
git add .
git commit -m "DS-9999 Add store metrics DAG and summary view"
```

### Step 5: Run the code review locally
Follow the instructions in `.claude/commands/code-review.md` to review the changes on this branch. Save the report to `code_reviews/code_review_feature-DS-9999-test-buggy-code.md`.

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
