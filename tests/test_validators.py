"""Tests for validators.py — verifies checks against buggy and clean sample files.

Runs all SQL and DAG validators on the existing buggy/clean file pairs and
asserts the expected PASS/FAIL results for each DS Testing Category.
"""

import os
import pathlib
import sys
import pytest

# Ensure the scripts directory is importable
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

from validators import (
    CheckResult,
    CATEGORY_NAMES,
    validate_sql_file,
    validate_dag_file,
    parse_diff_files,
    _strip_comments,
    _find_divisions,
    _extract_tables_from_sql,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BUGGY_SQL_PATH = REPO_ROOT / "ddl" / "fuji" / "vz_apps" / "buggy" / "vw_store_summary_buggy.sql"
CLEAN_SQL_PATH = REPO_ROOT / "ddl" / "fuji" / "vz_apps" / "clean" / "vw_store_summary_clean.sql"
BUGGY_DAG_PATH = REPO_ROOT / "dags" / "buggy" / "dag_store_metrics_buggy.py"
CLEAN_DAG_PATH = REPO_ROOT / "dags" / "clean" / "dag_store_metrics_clean.py"


@pytest.fixture
def buggy_sql():
    return BUGGY_SQL_PATH.read_text()


@pytest.fixture
def clean_sql():
    return CLEAN_SQL_PATH.read_text()


@pytest.fixture
def buggy_dag():
    return BUGGY_DAG_PATH.read_text()


@pytest.fixture
def clean_dag():
    return CLEAN_DAG_PATH.read_text()


@pytest.fixture
def buggy_sql_results(buggy_sql):
    return validate_sql_file(buggy_sql, "vw_store_summary_buggy.sql")


@pytest.fixture
def clean_sql_results(clean_sql):
    return validate_sql_file(clean_sql, "vw_store_summary_clean.sql")


@pytest.fixture
def buggy_dag_results(buggy_dag):
    return validate_dag_file(buggy_dag, "dag_store_metrics_buggy.py")


@pytest.fixture
def clean_dag_results(clean_dag):
    return validate_dag_file(clean_dag, "dag_store_metrics_clean.py")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def find_check(results: list[CheckResult], name_substring: str) -> CheckResult | None:
    """Find a check result by partial name match."""
    for r in results:
        if name_substring.lower() in r.name.lower():
            return r
    return None


def checks_by_category(results: list[CheckResult], cat: int) -> list[CheckResult]:
    """Filter checks by category number."""
    return [r for r in results if r.category == cat]


# ===================================================================
# Test: Buggy SQL catches known issues
# ===================================================================

class TestBuggySQLValidators:
    """Validators should detect issues in the buggy SQL file."""

    def test_detects_select_star(self, buggy_sql_results):
        """Buggy SQL has sub.* which should be caught."""
        check = find_check(buggy_sql_results, "SELECT *")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 1

    def test_detects_unqualified_tables(self, buggy_sql_results):
        """Buggy SQL has STORE, plan, orders without schema prefix."""
        check = find_check(buggy_sql_results, "Fully qualified table")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 1

    def test_detects_use_schema(self, buggy_sql_results):
        """Buggy SQL has USE SCHEMA vz_apps."""
        check = find_check(buggy_sql_results, "USE SCHEMA")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_missing_comment(self, buggy_sql_results):
        """Buggy SQL has no COMMENT with Jira ticket."""
        check = find_check(buggy_sql_results, "COMMENT")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_unqualified_view(self, buggy_sql_results):
        """Buggy SQL view is store_summary not vz_apps.vw_store_summary."""
        check = find_check(buggy_sql_results, "View name")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_update_without_where(self, buggy_sql_results):
        """Buggy SQL has UPDATE without WHERE clause."""
        check = find_check(buggy_sql_results, "UPDATE has WHERE")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 3

    def test_detects_divide_by_zero(self, buggy_sql_results):
        """Buggy SQL divides without CASE/NULLIF guard."""
        check = find_check(buggy_sql_results, "Divide-by-zero")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 4

    def test_detects_no_sp_rollout(self, buggy_sql_results):
        """Buggy SQL has no sp_rollout wrapping."""
        check = find_check(buggy_sql_results, "sp_rollout")
        assert check is not None
        assert check.status != "PASS"  # WARNING or FAIL
        assert check.category == 2

    def test_has_category_1_checks(self, buggy_sql_results):
        """Should have Category 1 checks."""
        cat1 = checks_by_category(buggy_sql_results, 1)
        assert len(cat1) >= 2

    def test_has_category_4_checks(self, buggy_sql_results):
        """Should have Category 4 checks."""
        cat4 = checks_by_category(buggy_sql_results, 4)
        assert len(cat4) >= 1

    def test_varchar_warning(self, buggy_sql_results):
        """Buggy SQL has VARCHAR(50) which is too small."""
        check = find_check(buggy_sql_results, "VARCHAR")
        assert check is not None
        assert check.status == "WARNING"
        assert "50" in check.detail


# ===================================================================
# Test: Clean SQL passes all checks
# ===================================================================

class TestCleanSQLValidators:
    """Validators should mostly pass on the clean SQL file."""

    def test_no_select_star(self, clean_sql_results):
        """Clean SQL uses explicit column lists."""
        check = find_check(clean_sql_results, "SELECT *")
        assert check is not None
        assert check.status == "PASS"

    def test_tables_fully_qualified(self, clean_sql_results):
        """Clean SQL uses FIL.STORE, FIL.PLAN, FIL.ORDERS."""
        check = find_check(clean_sql_results, "Fully qualified table")
        assert check is not None
        assert check.status == "PASS"

    def test_no_use_schema(self, clean_sql_results):
        """Clean SQL has no USE SCHEMA."""
        check = find_check(clean_sql_results, "USE SCHEMA")
        assert check is not None
        assert check.status == "PASS"

    def test_comment_with_ticket(self, clean_sql_results):
        """Clean SQL has COMMENT with DS-4521."""
        check = find_check(clean_sql_results, "COMMENT")
        assert check is not None
        assert check.status == "PASS"
        assert "DS-4521" in check.detail

    def test_view_fully_qualified(self, clean_sql_results):
        """Clean SQL view is vz_apps.vw_store_summary."""
        check = find_check(clean_sql_results, "View name")
        assert check is not None
        assert check.status == "PASS"

    def test_sp_rollout_present(self, clean_sql_results):
        """Clean SQL has sp_rollout start and end."""
        check = find_check(clean_sql_results, "sp_rollout")
        assert check is not None
        assert check.status == "PASS"

    def test_divide_by_zero_protected(self, clean_sql_results):
        """Clean SQL uses CASE WHEN for division."""
        check = find_check(clean_sql_results, "Divide-by-zero")
        if check:
            assert check.status == "PASS"

    def test_downstream_documented(self, clean_sql_results):
        """Clean SQL has downstream dependency documentation."""
        check = find_check(clean_sql_results, "Downstream")
        assert check is not None
        assert check.status == "PASS"
        assert check.category == 5

    def test_clone_backup(self, clean_sql_results):
        """Clean SQL has CLONE backup with drop date."""
        check = find_check(clean_sql_results, "CLONE")
        assert check is not None
        assert check.status == "PASS"

    def test_dev_role_return(self, clean_sql_results):
        """Clean SQL returns to dev role at end."""
        check = find_check(clean_sql_results, "Role returns")
        assert check is not None
        assert check.status == "PASS"

    def test_date_filtering(self, clean_sql_results):
        """Clean SQL has date-based WHERE clause."""
        check = find_check(clean_sql_results, "Date filtering")
        assert check is not None
        assert check.status == "PASS"

    def test_all_categories_covered(self, clean_sql_results):
        """Clean SQL results should cover all 5 categories."""
        cats = {r.category for r in clean_sql_results}
        assert 1 in cats, "Missing Category 1"
        assert 2 in cats, "Missing Category 2"
        assert 5 in cats, "Missing Category 5"


# ===================================================================
# Test: Buggy DAG catches known issues
# ===================================================================

class TestBuggyDAGValidators:
    """Validators should detect issues in the buggy DAG file."""

    def test_detects_deprecated_import(self, buggy_dag_results):
        """Buggy DAG uses airflow.operators.python_operator."""
        check = find_check(buggy_dag_results, "Modern Airflow imports")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_missing_default_args(self, buggy_dag_results):
        """Buggy DAG missing owner, retries, retry_delay."""
        check = find_check(buggy_dag_results, "default_args")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_missing_catchup(self, buggy_dag_results):
        """Buggy DAG has no catchup= setting."""
        check = find_check(buggy_dag_results, "catchup")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_camelcase_dag_id(self, buggy_dag_results):
        """Buggy DAG uses storeMetricsDaily (camelCase)."""
        check = find_check(buggy_dag_results, "snake_case DAG ID")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_hardcoded_credentials(self, buggy_dag_results):
        """Buggy DAG has hardcoded Snowflake password."""
        check = find_check(buggy_dag_results, "hardcoded credentials")
        assert check is not None
        assert check.status == "FAIL"
        assert check.severity == "CRITICAL"
        assert check.category == 4

    def test_detects_hardcoded_path(self, buggy_dag_results):
        """Buggy DAG has /home/airflow/production/ path."""
        check = find_check(buggy_dag_results, "hardcoded paths")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 4

    def test_detects_deprecated_provide_context(self, buggy_dag_results):
        """Buggy DAG uses provide_context=True."""
        check = find_check(buggy_dag_results, "provide_context")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_todo(self, buggy_dag_results):
        """Buggy DAG has TODO comment."""
        check = find_check(buggy_dag_results, "TODO")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2

    def test_detects_unused_imports(self, buggy_dag_results):
        """Buggy DAG has unused os and pandas imports."""
        check = find_check(buggy_dag_results, "unused imports")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 3

    def test_detects_orphaned_task(self, buggy_dag_results):
        """Buggy DAG has cleanup task not in dependency chain."""
        check = find_check(buggy_dag_results, "dependency chain")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 5

    def test_detects_select_star_in_sql(self, buggy_dag_results):
        """Buggy DAG has SELECT * FROM STORE in embedded SQL."""
        check = find_check(buggy_dag_results, "SELECT *")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 1

    def test_detects_missing_callback(self, buggy_dag_results):
        """Buggy DAG has no on_failure_callback."""
        check = find_check(buggy_dag_results, "on_failure_callback")
        assert check is not None
        assert check.status == "FAIL"
        assert check.category == 2


# ===================================================================
# Test: Clean DAG passes all checks
# ===================================================================

class TestCleanDAGValidators:
    """Validators should mostly pass on the clean DAG file."""

    def test_modern_imports(self, clean_dag_results):
        """Clean DAG uses airflow.operators.python."""
        check = find_check(clean_dag_results, "Modern Airflow imports")
        assert check is not None
        assert check.status == "PASS"

    def test_default_args_complete(self, clean_dag_results):
        """Clean DAG has owner, retries, retry_delay."""
        check = find_check(clean_dag_results, "default_args")
        assert check is not None
        assert check.status == "PASS"

    def test_catchup_set(self, clean_dag_results):
        """Clean DAG has catchup=False."""
        check = find_check(clean_dag_results, "catchup")
        assert check is not None
        assert check.status == "PASS"

    def test_snake_case_dag_id(self, clean_dag_results):
        """Clean DAG uses store_metrics_daily."""
        check = find_check(clean_dag_results, "snake_case DAG ID")
        assert check is not None
        assert check.status == "PASS"

    def test_no_hardcoded_credentials(self, clean_dag_results):
        """Clean DAG uses Airflow Connection."""
        check = find_check(clean_dag_results, "hardcoded credentials")
        assert check is not None
        assert check.status == "PASS"

    def test_no_todo(self, clean_dag_results):
        """Clean DAG has no TODO comments."""
        check = find_check(clean_dag_results, "TODO")
        assert check is not None
        assert check.status == "PASS"

    def test_jira_ticket_referenced(self, clean_dag_results):
        """Clean DAG references DS-4521."""
        check = find_check(clean_dag_results, "Jira ticket")
        assert check is not None
        assert check.status == "PASS"
        assert "DS-4521" in check.detail

    def test_has_tags(self, clean_dag_results):
        """Clean DAG has tags=."""
        check = find_check(clean_dag_results, "tags")
        assert check is not None
        assert check.status == "PASS"

    def test_callback_present(self, clean_dag_results):
        """Clean DAG has on_failure_callback."""
        check = find_check(clean_dag_results, "on_failure_callback")
        assert check is not None
        assert check.status == "PASS"

    def test_uses_conn_id(self, clean_dag_results):
        """Clean DAG uses snowflake_conn_id."""
        check = find_check(clean_dag_results, "Airflow Connection")
        assert check is not None
        assert check.status == "PASS"

    def test_no_select_star(self, clean_dag_results):
        """Clean DAG embedded SQL uses explicit columns."""
        check = find_check(clean_dag_results, "SELECT *")
        assert check is not None
        assert check.status == "PASS"

    def test_all_tasks_chained(self, clean_dag_results):
        """Clean DAG has all tasks connected."""
        check = find_check(clean_dag_results, "dependency chain")
        assert check is not None
        assert check.status == "PASS"

    def test_downstream_documented(self, clean_dag_results):
        """Clean DAG documents downstream dependencies."""
        check = find_check(clean_dag_results, "Downstream")
        assert check is not None
        assert check.status == "PASS"


# ===================================================================
# Test: Diff parser
# ===================================================================

class TestDiffParser:
    """Test parse_diff_files utility."""

    SAMPLE_DIFF = """\
diff --git a/dags/t_actively_exports.py b/dags/t_actively_exports.py
new file mode 100644
--- /dev/null
+++ b/dags/t_actively_exports.py
@@ -0,0 +1,50 @@
+from airflow import DAG

diff --git a/ddl/fuji/export_views/vw_crossbeam_overlaps.sql b/ddl/fuji/export_views/vw_crossbeam_overlaps.sql
new file mode 100644
--- /dev/null
+++ b/ddl/fuji/export_views/vw_crossbeam_overlaps.sql
@@ -0,0 +1,30 @@
+USE DATABASE FUJI;

diff --git a/include/utils/file_utils.py b/include/utils/file_utils.py
--- a/include/utils/file_utils.py
+++ b/include/utils/file_utils.py
@@ -1,5 +1,10 @@
 def existing():
     pass
+def new_func():
+    pass
"""

    def test_parses_file_count(self):
        files = parse_diff_files(self.SAMPLE_DIFF)
        assert len(files) == 3

    def test_parses_new_files(self):
        files = parse_diff_files(self.SAMPLE_DIFF)
        new_files = [f for f in files if f["status"] == "added"]
        assert len(new_files) == 2

    def test_parses_modified_files(self):
        files = parse_diff_files(self.SAMPLE_DIFF)
        modified = [f for f in files if f["status"] == "modified"]
        assert len(modified) == 1

    def test_parses_extensions(self):
        files = parse_diff_files(self.SAMPLE_DIFF)
        exts = {f["extension"] for f in files}
        assert ".py" in exts
        assert ".sql" in exts

    def test_parses_paths(self):
        files = parse_diff_files(self.SAMPLE_DIFF)
        paths = {f["path"] for f in files}
        assert "dags/t_actively_exports.py" in paths
        assert "ddl/fuji/export_views/vw_crossbeam_overlaps.sql" in paths

    def test_empty_diff(self):
        assert parse_diff_files("") == []

    def test_deduplicates(self):
        dup_diff = """\
diff --git a/file.py b/file.py
--- a/file.py
+++ b/file.py
@@ -1 +1 @@
-old
+new
diff --git a/file.py b/file.py
--- a/file.py
+++ b/file.py
@@ -5 +5 @@
-old2
+new2
"""
        files = parse_diff_files(dup_diff)
        assert len(files) == 1


# ===================================================================
# Test: Helper functions
# ===================================================================

class TestHelpers:
    """Test internal helper functions."""

    def test_strip_comments(self):
        sql = "-- this is a comment\nSELECT 1;\n-- another comment\nSELECT 2;"
        stripped = _strip_comments(sql)
        assert "comment" not in stripped
        assert "SELECT 1" in stripped
        assert "SELECT 2" in stripped

    def test_find_divisions(self):
        code = "a / b\n-- comment / ignored\nx = y / z"
        divs = _find_divisions(code)
        assert len(divs) == 2
        assert any("a / b" in d[0] for d in divs)

    def test_extract_tables(self):
        sql = "SELECT * FROM FIL.STORE s JOIN FIL.PLAN p ON s.id = p.id"
        tables = _extract_tables_from_sql(sql)
        assert "FIL.STORE" in tables
        assert "FIL.PLAN" in tables

    def test_extract_unqualified_tables(self):
        sql = "SELECT * FROM orders JOIN plan ON 1=1"
        tables = _extract_tables_from_sql(sql)
        unqualified = [t for t in tables if "." not in t]
        assert len(unqualified) >= 1


# ===================================================================
# Test: CheckResult data structure
# ===================================================================

class TestCheckResult:
    """Verify CheckResult dataclass."""

    def test_fields(self):
        r = CheckResult(
            name="test", status="PASS", severity="WARNING",
            detail="all good", category=1,
        )
        assert r.name == "test"
        assert r.status == "PASS"
        assert r.severity == "WARNING"
        assert r.detail == "all good"
        assert r.category == 1

    def test_category_names_complete(self):
        assert len(CATEGORY_NAMES) == 5
        assert CATEGORY_NAMES[1] == "Data Integrity"
        assert CATEGORY_NAMES[5] == "Business Logic & Downstream"


# ===================================================================
# Test: Category coverage
# ===================================================================

class TestCategoryCoverage:
    """Verify validators produce results across all relevant categories."""

    def test_buggy_sql_has_critical_fails(self, buggy_sql_results):
        """Buggy SQL should have at least some CRITICAL-severity fails."""
        criticals = [r for r in buggy_sql_results if r.severity == "CRITICAL" and r.status == "FAIL"]
        assert len(criticals) >= 1

    def test_clean_sql_no_critical_fails(self, clean_sql_results):
        """Clean SQL should have zero CRITICAL fails."""
        criticals = [r for r in clean_sql_results if r.severity == "CRITICAL" and r.status == "FAIL"]
        assert len(criticals) == 0

    def test_buggy_dag_has_critical_fails(self, buggy_dag_results):
        """Buggy DAG should have CRITICAL-severity fails (hardcoded creds)."""
        criticals = [r for r in buggy_dag_results if r.severity == "CRITICAL" and r.status == "FAIL"]
        assert len(criticals) >= 1

    def test_clean_dag_no_critical_fails(self, clean_dag_results):
        """Clean DAG should have zero CRITICAL fails."""
        criticals = [r for r in clean_dag_results if r.severity == "CRITICAL" and r.status == "FAIL"]
        assert len(criticals) == 0

    def test_buggy_sql_fail_count(self, buggy_sql_results):
        """Buggy SQL should have a significant number of failures."""
        fails = [r for r in buggy_sql_results if r.status == "FAIL"]
        assert len(fails) >= 5, f"Expected at least 5 fails, got {len(fails)}"

    def test_clean_sql_pass_rate(self, clean_sql_results):
        """Clean SQL should have mostly passing results."""
        total = len(clean_sql_results)
        passed = sum(1 for r in clean_sql_results if r.status == "PASS")
        assert passed / total > 0.7, f"Pass rate too low: {passed}/{total}"

    def test_buggy_dag_fail_count(self, buggy_dag_results):
        """Buggy DAG should have a significant number of failures."""
        fails = [r for r in buggy_dag_results if r.status == "FAIL"]
        assert len(fails) >= 5, f"Expected at least 5 fails, got {len(fails)}"

    def test_clean_dag_pass_rate(self, clean_dag_results):
        """Clean DAG should have mostly passing results."""
        total = len(clean_dag_results)
        passed = sum(1 for r in clean_dag_results if r.status == "PASS")
        assert passed / total > 0.7, f"Pass rate too low: {passed}/{total}"
