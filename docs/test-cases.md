# Test Cases — DS Automated Code Review

**Total tests:** 131
**Test runner:** pytest
**Run command:** `python3 -m pytest tests/ -v`

---

## Test File Overview

| File | Tests | What it validates |
|------|-------|-------------------|
| `test_dag_standards.py` | 40 | Python/Airflow DAG standards (buggy=20 issues, clean=20 checks) |
| `test_sql_standards.py` | 31 | SQL/Snowflake DDL/DML standards (buggy=18 issues, clean=13 checks) |
| `test_review_script.py` | 25 | review.py and generate_pr_description.py script structure |
| `test_edge_cases.py` | 35 | Edge case handling + repo structure validation |

---

## Testing Categories (5 DS Validation Types)

The test suite is organized around 5 high-level testing categories identified from the DS team's 2025 rollout and testing practices:

1. **Data Integrity** — HASH_AGG fingerprinting, row counts, MINUS comparisons
2. **Schema & DDL Compliance** — sp_rollout wrapping, COMMENT with ticket, information_schema validation
3. **Regression** — HASH_AGG EXCLUDE for unchanged columns, cross-environment DEV vs PROD comparisons
4. **Edge Cases** — NULL handling, divide-by-zero, duplicates, ambiguity in ROW_NUMBER, VARCHAR truncation
5. **Business Logic & Downstream** — security.table_usage_summary checks, financial metric reconciliation

---

## 1. Python / Airflow DAG Standards (`test_dag_standards.py`)

### Buggy DAG — 20 Issues Detected

| # | Test | DS Standard | Severity |
|---|------|-------------|----------|
| 1 | `test_bug01_deprecated_import` | Uses deprecated `airflow.operators.python_operator` | WARNING |
| 2 | `test_bug02_unused_import_os` | `import os` present but never used | WARNING |
| 3 | `test_bug03_unused_import_pandas` | `import pandas` present but never used | WARNING |
| 4 | `test_bug04_missing_default_args_fields` | `default_args` missing `owner`, `retries`, `retry_delay` | CRITICAL |
| 5 | `test_bug05_no_catchup_setting` | `catchup` not explicitly set (defaults to True) | WARNING |
| 6 | `test_bug06_camelcase_dag_id` | DAG ID `storeMetricsDaily` uses camelCase, not snake_case | WARNING |
| 7 | `test_bug07_hardcoded_credentials` | Hardcoded Snowflake password in source code | CRITICAL |
| 8 | `test_bug08_no_empty_result_handling` | No check for empty result set from Snowflake | WARNING |
| 9 | `test_bug09_large_xcom_push` | Pushes unbounded data via XCom | WARNING |
| 10 | `test_bug10_select_star_and_unqualified_table` | `SELECT * FROM STORE` — unqualified + SELECT * | CRITICAL |
| 11 | `test_bug11_no_null_check_on_data` | Iterates `data` without null check | WARNING |
| 12 | `test_bug12_divide_by_zero` | `revenue / cost` with no zero guard | WARNING |
| 13 | `test_bug13_todo_left_in_code` | `TODO:` comment left in production code | SUGGESTION |
| 14 | `test_bug14_hardcoded_path` | Hardcoded `/home/airflow/production/` path | CRITICAL |
| 15 | `test_bug15_deprecated_provide_context` | Uses deprecated `provide_context=True` | WARNING |
| 16 | `test_bug16_no_failure_callback` | No `on_failure_callback` on any task | WARNING |
| 17 | `test_bug17_orphaned_task` | `cleanup` task not in dependency chain | WARNING |
| 18 | `test_bug18_no_validation_task` | No data integrity validation task after load | WARNING |
| 19 | `test_bug19_no_regression_check` | No HASH_AGG or regression comparison task | WARNING |
| 20 | `test_bug20_no_downstream_awareness` | No downstream dependency documentation | WARNING |

### Clean DAG — 20 Checks Passed

| Test | DS Standard |
|------|-------------|
| `test_modern_import` | Uses `airflow.operators.python` (not deprecated) |
| `test_no_unused_imports` | No unused imports |
| `test_default_args_complete` | `default_args` has `owner`, `start_date`, `retries`, `retry_delay` |
| `test_catchup_false` | `catchup=False` explicitly set |
| `test_snake_case_dag_id` | DAG ID uses snake_case |
| `test_no_hardcoded_credentials` | No passwords or secrets in source |
| `test_uses_airflow_connection` | Uses `snowflake_conn_id` (Airflow Connection) |
| `test_on_failure_callback_present` | `on_failure_callback` configured |
| `test_no_provide_context` | No deprecated `provide_context` |
| `test_no_orphaned_tasks` | All tasks connected: `extract >> transform >> validate >> regression_check` |
| `test_fully_qualified_table_names` | SQL uses `FIL.STORE`, `FIL.STORE_METRICS_DAILY` |
| `test_divide_by_zero_protection` | `CASE WHEN total_orders > 0` guards division |
| `test_null_handling_with_coalesce` | `COALESCE` wraps nullable fields |
| `test_no_select_star` | No `SELECT *` in SQL blocks |
| `test_no_todo_comments` | No `TODO`/`FIXME` in code |
| `test_jira_ticket_in_docstring` | DS-4521 referenced in docstring |
| `test_has_dag_tags` | DAG has `tags=` for discoverability |
| `test_has_validation_task` | Data integrity validation task after transform |
| `test_has_regression_check` | Regression check task compares today vs yesterday |
| `test_downstream_dependencies_documented` | Downstream consumers documented in comments |

---

## 2. SQL / Snowflake DDL/DML Standards (`test_sql_standards.py`)

### Buggy SQL — 18 Issues Detected

| # | Test | DS Standard | Severity |
|---|------|-------------|----------|
| 1 | `test_bug01_missing_use_database` | No `USE DATABASE FUJI` statement | WARNING |
| 2 | `test_bug02_has_use_schema` | Contains `USE SCHEMA` (forbidden in DDL) | CRITICAL |
| 3 | `test_bug03_missing_object_comment` | No `COMMENT` with Jira ticket on view | CRITICAL |
| 4 | `test_bug04_view_not_fully_qualified` | View name not schema-qualified | CRITICAL |
| 5 | `test_bug05_select_star_from_subquery` | Uses `sub.*` (SELECT * from subquery) | WARNING |
| 6 | `test_bug06_divide_by_zero_risk` | `total_revenue / total_orders` with no guard | WARNING |
| 7 | `test_bug07_null_propagation_left_join` | LEFT JOIN columns without `COALESCE` | WARNING |
| 8 | `test_bug08_unqualified_store_table` | `FROM STORE` — missing `FIL.` prefix | CRITICAL |
| 9 | `test_bug09_unqualified_plan_table` | `JOIN plan` — missing `FIL.` prefix | CRITICAL |
| 10 | `test_bug10_unqualified_orders_table` | `FROM orders` — missing `FIL.` prefix | CRITICAL |
| 11 | `test_bug11_subquery_missing_where` | Orders subquery has no WHERE (full table scan) | WARNING |
| 12 | `test_bug12_inner_join_drops_zero_order_stores` | INNER JOIN silently drops stores with no orders | WARNING |
| 13 | `test_bug13_update_without_where` | `UPDATE` without `WHERE` clause | CRITICAL |
| 14 | `test_bug14_missing_sp_rollout` | No sp_rollout wrapping for rollout SQL | CRITICAL |
| 15 | `test_bug15_no_backup_before_change` | No CLONE backup before destructive change | WARNING |
| 16 | `test_bug16_nondeterministic_row_number` | ROW_NUMBER ORDER BY lacks tiebreaker — ambiguity risk | WARNING |
| 17 | `test_bug17_varchar_too_small` | VARCHAR(50) too small for store descriptions | WARNING |
| 18 | `test_bug18_no_downstream_check` | No downstream dependency check documented | WARNING |

### Clean SQL — 13 Checks Passed

| Test | DS Standard |
|------|-------------|
| `test_has_use_database` | `USE DATABASE FUJI` present |
| `test_no_use_schema` | No `USE SCHEMA` statement |
| `test_view_fully_qualified` | View name is `vz_apps.vw_store_summary` |
| `test_object_comment_with_ticket` | `COMMENT = 'DS-4521 ...'` present |
| `test_all_tables_fully_qualified` | All tables use `FIL.` prefix |
| `test_no_select_star` | No `SELECT *` or `alias.*` |
| `test_divide_by_zero_protection` | `CASE WHEN COALESCE(...) > 0` guards division |
| `test_null_handling_with_coalesce` | All nullable columns wrapped in `COALESCE` |
| `test_left_join_for_optional_data` | All JOINs are LEFT JOIN |
| `test_subquery_has_where_clause` | Orders subquery filters by date |
| `test_has_active_filter` | Filters `is_active = TRUE` |
| `test_no_update_without_where` | No unguarded UPDATE/DELETE |
| `test_has_sp_rollout_wrapping` | Rollout SQL wrapped with sp_rollout start/end |
| `test_has_backup_clone` | Backup created via CLONE before changes |
| `test_clone_has_drop_date_comment` | CLONE backup has drop date in comment |
| `test_deterministic_row_number` | ROW_NUMBER has deterministic ORDER BY with tiebreaker |
| `test_adequate_varchar_length` | VARCHAR length adequate (not truncation-prone) |
| `test_downstream_check_documented` | Downstream dependency check documented |
| `test_role_returns_to_dev` | Rollout ends with dev role switch |

---

## 3. Review Script Validation (`test_review_script.py`)

### review.py — 19 Tests

| Test | What it validates |
|------|-------------------|
| `test_script_parses_as_valid_python` | Script is syntactically valid |
| `test_uses_openai_client` | Uses `openai` package for API calls |
| `test_reads_diff_from_tmp` | Reads diff from `/tmp/diff.txt` |
| `test_reads_context_md` | Loads project context from `CONTEXT.md` |
| `test_has_max_diff_limit` | Truncates large diffs to prevent token overflow |
| `test_handles_empty_diff` | Returns safe message for empty diffs |
| `test_handles_missing_diff_file` | Exits with error when diff file missing |
| `test_supports_multiple_api_keys` | Checks OPENAI, OPENROUTER, and ANTHROPIC keys |
| `test_prompt_covers_ds_workflow_compliance` | Prompt checks for Jira ticket |
| `test_prompt_covers_python_checks` | Prompt covers catchup, default_args, callbacks, creds |
| `test_prompt_covers_sql_checks` | Prompt covers qualified names, WHERE, COMMENT |
| `test_prompt_includes_severity_levels` | Prompt uses CRITICAL/WARNING/SUGGESTION/PASS |
| `test_prompt_includes_sox_reminder` | Prompt includes SOX compliance note |
| `test_prompt_requests_test_cases` | Prompt asks for generated test cases + validation |
| `test_prompt_covers_rollout_standards` | Prompt checks sp_rollout wrapping and dev role |
| `test_prompt_covers_five_testing_categories` | Prompt references all 5 DS testing categories |
| `test_prompt_covers_hash_agg` | Prompt mentions HASH_AGG for data comparison |
| `test_configurable_model` | Model configurable via `REVIEW_MODEL` env var |
| `test_configurable_base_url` | Base URL configurable via `API_BASE_URL` env var |

### generate_pr_description.py — 6 Tests

| Test | What it validates |
|------|-------------------|
| `test_script_parses_as_valid_python` | Script is syntactically valid |
| `test_reads_diff_and_commits` | Reads both diff and commit messages |
| `test_reads_pr_template` | Loads PR template from `.github/` |
| `test_uses_pr_metadata` | Uses PR title and branch name |
| `test_handles_missing_diff` | Exits with error when diff unavailable |
| `test_instructs_jira_ticket_extraction` | Prompt extracts ticket from branch/commits |

---

## 4. Edge Cases and Repo Structure (`test_edge_cases.py`)

### Buggy DAG — Edge Case Failures (5 tests)

| Test | Edge case not handled |
|------|----------------------|
| `test_no_empty_dataset_handling` | `fetchall()` result not checked for empty |
| `test_no_null_check_before_iteration` | Iterates `data` without None check |
| `test_no_divide_by_zero_guard` | `revenue / cost` with no zero check |
| `test_no_xcom_size_guard` | Pushes unbounded data to XCom |
| `test_no_upstream_failure_handling` | No check for missing XCom from failed extract |

### Clean DAG — Edge Cases Handled (5 tests)

| Test | Edge case handled |
|------|-------------------|
| `test_divide_by_zero_protected` | `CASE WHEN total_orders > 0` |
| `test_null_values_coalesced` | `COALESCE(total_orders, 0)` |
| `test_validation_step_catches_empty_load` | `WHEN COUNT(*) = 0 THEN 1/0` fails task |
| `test_date_filtering_uses_template` | Uses `{{ ds }}` Airflow template |
| `test_retry_configured` | `retries: 2`, `retry_delay` set |

### Buggy SQL — Edge Case Failures (6 tests)

| Test | Edge case not handled |
|------|----------------------|
| `test_divide_by_zero_unprotected` | Raw division with no guard |
| `test_null_propagation_from_left_join` | LEFT JOIN columns used without COALESCE |
| `test_inner_join_drops_rows` | INNER JOIN silently drops zero-order stores |
| `test_full_table_scan_on_orders` | Orders subquery has no WHERE clause |
| `test_update_affects_all_rows` | UPDATE with no WHERE modifies every row |
| `test_no_duplicate_handling` | No DISTINCT or QUALIFY deduplication logic |

### Clean SQL — Edge Cases Handled (5 tests)

| Test | Edge case handled |
|------|-------------------|
| `test_divide_by_zero_protected` | `CASE WHEN COALESCE(...) > 0` |
| `test_null_coalesced_from_joins` | All nullable columns use `COALESCE` |
| `test_left_join_preserves_all_stores` | All JOINs are LEFT JOIN |
| `test_date_bounded_query` | `DATEADD('year', -1, CURRENT_DATE())` |
| `test_active_store_filter` | `is_active = TRUE` filter |

### Repo Structure (8 tests)

| Test | What it validates |
|------|-------------------|
| `test_context_md_exists` | `CONTEXT.md` present at repo root |
| `test_review_instructions_exist` | `docs/code-review-instructions.md` exists |
| `test_workflow_exists` | `code-review.yml` workflow exists |
| `test_pr_description_workflow_exists` | `pr-description.yml` workflow exists |
| `test_review_script_exists` | `review.py` script exists |
| `test_pr_desc_script_exists` | `generate_pr_description.py` exists |
| `test_gitignore_has_code_reviews` | `code_reviews/` is gitignored |
| `test_pr_template_exists` | PR description template exists |
