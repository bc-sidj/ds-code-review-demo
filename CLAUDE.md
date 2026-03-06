# DS Code Review Demo — Project Context

This is a proof-of-concept repo for automated code review on the BigCommerce Data Solutions (DS) team. It mirrors the structure of the production `dw_airflow` repo. Developers use **Cursor** as their IDE for local development and code review.

## Repository Structure
- `dags/` — Airflow DAG Python files
- `ddl/fuji/` — Snowflake DDL/DML files organized by database/schema (e.g., `ddl/fuji/vz_apps/`)
- `code_reviews/` — Local review reports generated during code review (gitignored)
- `.claude/commands/code-review.md` — Code review instructions (reference prompt for Cursor AI)
- `.github/workflows/claude-code-review.yml` — GitHub Action for PR-level review (uses Anthropic API)
- `.github/scripts/review.py` — Python script that calls the Anthropic API for automated PR review

## DS Team Standards

### Git & PR Rules
- Every PR must link to a Jira ticket (DS-XXXX or ANALYTICS-XXXX)
- At least one commit message must include the ticket number: `DS-1234 message`
- Testing must be complete before PR creation, with documentation saved in Google Drive
- A rollout doc is required if there are DDL/DML or complex Airflow changes
- Separation of duties: the PR submitter cannot be the person who rolls out to production (SOX compliance)

### Python / Airflow Standards
- Use `catchup=False` unless backfill is explicitly intended
- `default_args` must include: `owner`, `start_date`, `retries`, `retry_delay`
- All tasks should have `on_failure_callback` for critical DAGs
- Never hardcode credentials — use Airflow Variables and Connections
- DAG IDs and task IDs use `snake_case`
- No unused imports
- DAGs must be idempotent (safe to re-run)

### SQL / Snowflake Standards
- All object references must be fully qualified: `FIL.STORE` not just `STORE`
- Never use `USE SCHEMA` in DDL files
- Every created/modified object must have a `COMMENT` with the Jira ticket: `COMMENT = 'DS-XXXX description'`
- All `UPDATE`/`DELETE` must have a `WHERE` clause
- Rollout docs must start with `CALL edw.gbl.sp_rollout('start', ...)` and end with `CALL edw.gbl.sp_rollout('end', ...)`
- Backout tables use `CLONE` and include a comment with ticket + drop date
- SQL file names are lowercase and match the object name exactly

### Edge Cases to Always Check
- Empty datasets / zero rows from upstream
- NULL values in key fields
- Divide-by-zero in calculated columns
- NULL propagation in LEFT JOINs (use COALESCE)
- Duplicate handling
- Date boundaries (off-by-one, timezone, DST)
- Large volume scenarios
