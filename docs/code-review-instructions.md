# DS Code Review

Perform a thorough automated code review of all changes on the current branch before a Pull Request is created. This is a first-pass review covering DS workflow compliance, code quality, edge cases, and test case generation.

## Steps

### 1. Gather branch context
Run the following and capture the output:
```bash
git rev-parse --abbrev-ref HEAD
git log main...HEAD --oneline
git diff main...HEAD --name-only
git diff main...HEAD
```

Store the branch name, commit messages, changed file list, and full diff.

### 2. DS Workflow Compliance Check
Check the commit messages for a Jira ticket number in the format `DS-XXXX` or `ANALYTICS-XXXX`.
- If no ticket number is found: flag as ❌ FAIL with message "No Jira ticket number found in commit messages. At least one commit must include DS-XXXX."
- If found: flag as ✅ PASS

Check the changed files:
- If any `.sql` files are changed: flag as ⚠️ REMINDER "DDL/DML changes detected — ensure a rollout document and testing doc are saved to Google Drive at: Ops & Analytics/Data Solutions/Unit Testing + Rollout/"
- If only `.sql` files and no `.py` files: flag as ℹ️ INFO "DDL-only change — consider using the Simplified PR process."
- If `.py` files are changed: flag as ⚠️ REMINDER "Airflow code changes detected — ensure all touched Airflow jobs have been tested with the exact version going to production."

### 3. Python / Airflow DAG Review
For every `.py` file in the diff, perform the following checks and flag each as ✅ PASS, ⚠️ WARNING, or ❌ ISSUE:

**DAG Structure:**
- `catchup` is explicitly set (preferably `catchup=False` unless backfill is intentional)
- `default_args` includes `owner`, `start_date`, `retries`, `retry_delay`
- `schedule_interval` is defined and appropriate
- DAG ID uses snake_case and is descriptive

**Task Dependencies & Logic:**
- Task dependencies use `>>` / `<<` correctly with no circular chains
- All tasks are connected — no orphaned tasks
- Correct operator used for the task type (e.g., `PythonOperator`, `SnowflakeOperator`, `BashOperator`)
- XCom usage is intentional and not passing large data

**Error Handling & Resilience:**
- `retries` and `retry_delay` are set on tasks or in `default_args`
- `on_failure_callback` is present on critical tasks
- DAG can be safely re-run (idempotent) — no side effects from duplicate runs

**Security & Config:**
- No hardcoded credentials, passwords, or secrets anywhere in the code
- Airflow Variables and Connections used for environment-specific config
- No hardcoded environment-specific paths or hostnames

**Code Quality:**
- No unused imports
- No missing imports
- Functions are small and single-purpose
- No TODO/FIXME left in the code
- Naming follows snake_case convention

**Edge Cases — flag any of the following if not handled:**
- Empty dataset / no rows returned from upstream
- NULL values in key fields used in conditionals or joins
- Date range issues (off-by-one, timezone handling, DST)
- Large volume scenarios (will this work at 10x the expected data size?)
- Upstream task failure propagation

### 4. SQL / DDL / DML Review
For every `.sql` file in the diff, perform the following checks:

**Object Naming & Standards:**
- All object references are fully qualified (e.g., `FIL.STORE`, not just `STORE`)
- No `USE SCHEMA` statements
- `USE DATABASE FUJI` or appropriate database is specified where needed
- Object names use lowercase in file names and are exact matches to database objects

**DDL Standards:**
- `CREATE OR REPLACE` used appropriately
- Object `COMMENT` includes the Jira ticket number: `COMMENT = 'DS-XXXX description'`
- Column data types are appropriate and consistent with existing schema patterns
- NULLability (`NOT NULL`) is intentional and justified

**DML Safety:**
- All `UPDATE`/`DELETE` statements have a `WHERE` clause — flag as ❌ CRITICAL ISSUE if any are missing
- Potential for large full-table scans flagged as ⚠️ WARNING
- Transactions used where multiple DML statements must succeed or fail together

**Performance:**
- Missing `WHERE` clauses on large tables flagged
- Unnecessary `SELECT *` flagged
- CTEs are used for readability where appropriate
- Window functions used correctly (PARTITION BY, ORDER BY)

**Rollout Readiness:**
- If a rollout doc is being reviewed: `sp_rollout('start', ...)` and `sp_rollout('end', ...)` calls present
- Backout table (if present) uses `CLONE` and has a `COMMENT` with ticket + expiry date (e.g., `'Drop after YYYY-MM-DD'`)
- Rollout can be safely run in order without manual intervention
- Multi-step rollouts must have ordered comments: `--Step 1:`, `--Step 2:`, etc.
- Role switching: must return to dev role (e.g., `USE ROLE FUJI_DEV_OWNER`) at end of rollout
- Stash pattern: deprecated objects should be moved to `FUJI_STASH` before dropping

**Edge Cases — flag any of the following if not handled:**
- NULL propagation in JOINs (LEFT vs INNER correctness)
- Duplicate rows — is deduplication logic needed?
- Divide-by-zero in any calculated columns
- Empty result set scenarios — does downstream logic handle zero rows?
- Data type mismatches in JOINs or comparisons
- Field length truncation — will new data fit in existing VARCHAR columns?
- Non-deterministic ordering in QUALIFY/ROW_NUMBER (ambiguity risk)

### 5. Test Case Generation

Generate test stubs organized by the **5 DS Testing Categories**:

#### Category 1: Data Integrity Validation
For each changed **SQL file**, generate:
1. **HASH_AGG fingerprint** — `SELECT HASH_AGG(*) FROM <table>` before and after, compare
2. **Row count check** — `SELECT COUNT(*) FROM <table>` and `SELECT COUNT(DISTINCT <key>) FROM <table>` before and after
3. **MINUS comparison** — `SELECT * FROM <new_table> MINUS SELECT * FROM <old_table>` to find exact diffs
4. **Column-level diff** — `SELECT HASH_AGG(* EXCLUDE (<changed_cols>)) FROM <table>` to isolate changes

#### Category 2: Schema & DDL Compliance
For each changed **SQL file**, generate:
1. **Schema inspection** — `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'X' AND table_name = 'Y'`
2. **Comment verification** — confirm `COMMENT` includes Jira ticket
3. **sp_rollout wrapping** — verify rollout bookends are present

#### Category 3: Regression Testing
For each changed file, generate:
1. **Unchanged data check** — `HASH_AGG(* EXCLUDE (<changed_columns>))` comparing DEV vs PROD
2. **Cross-environment comparison** — `SELECT 'PROD', COUNT(*), HASH_AGG(*) FROM fuji.<table> UNION ALL SELECT 'DEV', COUNT(*), HASH_AGG(*) FROM fuji_dev.<table>`
3. **Historical preservation** — verify prior-period data unchanged

#### Category 4: Edge Case & Guard Rail Tests
For each changed **Python file**, generate `pytest` stubs for:
1. **Empty input** — function/task handles zero records gracefully
2. **NULL values** — key fields containing NULL don't cause crashes
3. **Date boundaries** — start/end of month, year, DST transitions
4. **Upstream failure** — task behaves correctly when upstream fails or returns no XCom
5. **Divide-by-zero** — calculations protected by CASE WHEN or NULLIF
6. **Ambiguity** — ROW_NUMBER/QUALIFY has deterministic ORDER BY

For each changed **SQL file**, generate:
1. **NULL check** on primary/key columns: `SELECT COUNT(*) FROM <table> WHERE <key_col> IS NULL`
2. **Duplicate check**: `SELECT <key_col>, COUNT(*) FROM <table> GROUP BY 1 HAVING COUNT(*) > 1`
3. **Range/sanity check** on numeric columns: `SELECT MIN(<col>), MAX(<col>), AVG(<col>) FROM <table>`

#### Category 5: Business Logic & Downstream Validation
For each change, generate:
1. **Downstream dependency check** — `SELECT * FROM security.table_usage_summary WHERE full_table_name = UPPER('<table>') AND user_name IN ('AIRFLOW', 'TABLEAU', 'TABLEAU_2')`
2. **Spot-check**: `SELECT * FROM <table> WHERE <key_col> IN (<known_test_values>) LIMIT 10`
3. **Aggregate validation** — for financial tables, verify SUM/COUNT of key metrics before/after

### 6. Write the Review Report

Get the current branch name and write the complete review report to:
`code_reviews/code_review_<branch-name>.md`

The report must follow this structure:

```markdown
# Code Review Report
**Branch:** <branch-name>
**Reviewed at:** <timestamp>
**Changed files:** <list>
**Commits reviewed:** <list>

---

## DS Workflow Compliance
| Check | Status | Notes |
|-------|--------|-------|
| Jira ticket in commit | ✅/❌ | ... |
| Testing doc reminder | ⚠️/✅ | ... |
| Rollout doc reminder | ⚠️/✅ | ... |

---

## Code Review Findings

### [filename]
#### Critical Issues ❌
- ...
#### Warnings ⚠️
- ...
#### Passed Checks ✅
- ...

---

## Edge Cases Flagged
- ...

---

## Generated Test Cases

### Python Unit Tests
(pytest stubs)

### SQL Validation Queries
(validation query stubs)

---

## Summary
**Overall Status:** ✅ READY FOR PR / ⚠️ REVIEW REQUIRED / ❌ ISSUES MUST BE FIXED
**Critical issues:** <count>
**Warnings:** <count>
**Passed checks:** <count>
```

After writing the file, tell the user:
- The report has been saved to `code_reviews/code_review_<branch>.md`
- How many critical issues, warnings, and passed checks were found
- Whether the code is ready for a PR or needs fixes first
