# DS Code Review Demo — Project Context

This is a proof-of-concept repo for automated code review on the BigCommerce Data Solutions (DS) team. It mirrors the structure of the production `dw_airflow` repo. Developers use **Claude** for AI-assisted local development and code review.

## Repository Structure
- `dags/` — Airflow DAG Python files
- `ddl/fuji/` — Snowflake DDL/DML files organized by database/schema (e.g., `ddl/fuji/vz_apps/`)
- `code_reviews/` — Local review reports generated during code review (gitignored)
- `docs/code-review-instructions.md` — Code review instructions (reference prompt for Claude)
- `.github/workflows/code-review.yml` — GitHub Action for PR-level review
- `.github/scripts/review.py` — Python script that calls an OpenAI-compatible API (OpenRouter by default) for automated PR review

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

### Rollout Standards
- Every rollout SQL must be wrapped with `CALL edw.gbl.sp_rollout('start', ...)` and `CALL edw.gbl.sp_rollout('end', ...)`
- Backout/backup tables use `CLONE` with a `COMMENT` containing ticket + expiry date (e.g., `'Drop after YYYY-MM-DD'`)
- Multi-step rollouts should be ordered and commented: `--Step 1:`, `--Step 2:`, etc.
- Role switching must return to dev role after production operations (e.g., `USE ROLE FUJI_DEV_OWNER` at end)
- Stash pattern: deprecated objects moved to `FUJI_STASH` schema before dropping

### Testing Categories (5 Required Validation Types)

#### 1. Data Integrity Validation
- Use `HASH_AGG(*)` to compare full table fingerprints before/after changes
- Use `HASH_AGG(* EXCLUDE (col1, col2))` to isolate changed vs unchanged columns
- Row count validation: `SELECT COUNT(*)` and `SELECT COUNT(DISTINCT key)` before and after
- Use `MINUS` / `EXCEPT` to find exact row-level differences between environments
- Every data-modifying PR must show before/after evidence

#### 2. Schema & DDL Compliance
- Verify column metadata: data types, comments, NULLability match expectations
- Use `information_schema.columns` or `DESC TABLE` to validate schema changes
- Every new/modified object must have `COMMENT` with Jira ticket
- sp_rollout wrapping required for all production DDL/DML
- Verify fully qualified names, no USE SCHEMA, proper role switching

#### 3. Regression Testing
- For logic changes: `HASH_AGG(* EXCLUDE (changed_columns))` must match production on unchanged columns
- For refactors/style-only changes: `HASH_AGG(*)` must match exactly (zero diff)
- Cross-environment comparison: DEV vs PROD using `UNION ALL` with environment labels
- Historical data preservation: verify prior-period data unchanged after backfill

#### 4. Edge Case & Guard Rail Testing
- Empty datasets / zero rows from upstream
- NULL values in key fields
- Divide-by-zero in calculated columns (use CASE WHEN or NULLIF)
- NULL propagation in LEFT JOINs (use COALESCE)
- Duplicate detection: `GROUP BY ... HAVING COUNT(*) > 1`
- Date boundaries (off-by-one, timezone, DST)
- Field length truncation (VARCHAR overflow)
- Non-deterministic ordering / ambiguity in QUALIFY/ROW_NUMBER
- Large volume scenarios

#### 5. Business Logic & Downstream Validation
- Verify downstream dependencies are not broken: query `security.table_usage_summary` for AIRFLOW/TABLEAU users
- Search GitHub for references to modified/deprecated objects
- For financial metrics: before/after comparison of aggregate values (MRR, ARR, revenue)
- Spot-check specific records by tracing logic manually (pick known account_key values)
- Validate attribution logic sums to 100% where applicable
- Check Tableau/dashboard impact when dropping or renaming columns
