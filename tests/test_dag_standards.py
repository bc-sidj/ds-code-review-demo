"""Tests for Python/Airflow DAG standards.

Validates that:
- The buggy DAG contains all 17 known issues
- The clean DAG passes all DS team checks
"""

import ast
import re
import pytest

from conftest import BUGGY_DAG_PATH, CLEAN_DAG_PATH


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_ast(source: str) -> ast.Module:
    return ast.parse(source)


def get_imports(source: str) -> list[str]:
    """Return all import lines from source."""
    return [line.strip() for line in source.splitlines() if line.strip().startswith(("import ", "from "))]


def get_default_args_keys(source: str) -> set[str]:
    """Extract keys from the default_args dict."""
    tree = parse_ast(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "default_args":
                    if isinstance(node.value, ast.Dict):
                        return {
                            key.value for key in node.value.keys
                            if isinstance(key, ast.Constant)
                        }
    return set()


# ===========================================================================
# BUGGY DAG — Should contain all 17 issues
# ===========================================================================

class TestBuggyDagIssues:
    """Verify the buggy DAG contains all 17 intentional issues."""

    def test_bug01_deprecated_import(self, buggy_dag_source):
        """BUG 1: Uses deprecated airflow.operators.python_operator."""
        assert "from airflow.operators.python_operator import" in buggy_dag_source

    def test_bug02_unused_import_os(self, buggy_dag_source):
        """BUG 2: Imports os but never uses it."""
        assert "import os" in buggy_dag_source
        lines_using_os = [
            line for line in buggy_dag_source.splitlines()
            if "os." in line and not line.strip().startswith(("import ", "#"))
        ]
        assert len(lines_using_os) == 0

    def test_bug03_unused_import_pandas(self, buggy_dag_source):
        """BUG 3: Imports pandas but never uses it."""
        assert "import pandas" in buggy_dag_source
        lines_using_pd = [
            line for line in buggy_dag_source.splitlines()
            if "pd." in line and not line.strip().startswith(("import ", "#"))
        ]
        assert len(lines_using_pd) == 0

    def test_bug04_missing_default_args_fields(self, buggy_dag_source):
        """BUG 4: default_args missing owner, retries, retry_delay."""
        keys = get_default_args_keys(buggy_dag_source)
        assert "owner" not in keys
        assert "retries" not in keys
        assert "retry_delay" not in keys

    def test_bug05_no_catchup_setting(self, buggy_dag_source):
        """BUG 5: catchup is not explicitly set."""
        code_lines = [l for l in buggy_dag_source.splitlines() if not l.strip().startswith("#")]
        code_only = "\n".join(code_lines)
        assert "catchup=" not in code_only and "catchup =" not in code_only

    def test_bug06_camelcase_dag_id(self, buggy_dag_source):
        """BUG 6: DAG ID uses camelCase instead of snake_case."""
        match = re.search(r"""['"]storeMetricsDaily['"]""", buggy_dag_source)
        assert match is not None

    def test_bug07_hardcoded_credentials(self, buggy_dag_source):
        """BUG 7: Contains hardcoded password — CRITICAL."""
        assert "Sup3rS3cretP@ss!" in buggy_dag_source

    def test_bug08_no_empty_result_handling(self, buggy_dag_source):
        """BUG 8: extract_store_data has no check for empty results."""
        extract_func = buggy_dag_source.split("def extract_store_data")[1].split("\ndef ")[0]
        assert "if not" not in extract_func
        assert "is None" not in extract_func
        assert "len(" not in extract_func

    def test_bug09_large_xcom_push(self, buggy_dag_source):
        """BUG 9: Pushes potentially large result set via XCom."""
        assert "xcom_push(key='store_data', value=results)" in buggy_dag_source

    def test_bug10_select_star_and_unqualified_table(self, buggy_dag_source):
        """BUG 10: Uses SELECT * and unqualified table name STORE."""
        assert 'SELECT * FROM STORE' in buggy_dag_source

    def test_bug11_no_null_check_on_data(self, buggy_dag_source):
        """BUG 11: transform_store_data iterates data without null check."""
        transform_func = buggy_dag_source.split("def transform_store_data")[1].split("\ndef ")[0]
        assert "if data is None" not in transform_func
        assert "if not data" not in transform_func

    def test_bug12_divide_by_zero(self, buggy_dag_source):
        """BUG 12: revenue / cost with no zero check."""
        assert "revenue / cost" in buggy_dag_source
        transform_func = buggy_dag_source.split("def transform_store_data")[1].split("\ndef ")[0]
        assert "cost == 0" not in transform_func
        assert "cost > 0" not in transform_func

    def test_bug13_todo_left_in_code(self, buggy_dag_source):
        """BUG 13: TODO comment left in production code."""
        assert "TODO:" in buggy_dag_source

    def test_bug14_hardcoded_path(self, buggy_dag_source):
        """BUG 14: Hardcoded environment-specific path."""
        assert "/home/airflow/production/" in buggy_dag_source

    def test_bug15_deprecated_provide_context(self, buggy_dag_source):
        """BUG 15: Uses deprecated provide_context=True."""
        assert "provide_context=True" in buggy_dag_source

    def test_bug16_no_failure_callback(self, buggy_dag_source):
        """BUG 16: No on_failure_callback on any task."""
        code_lines = [l for l in buggy_dag_source.splitlines() if not l.strip().startswith("#")]
        code_only = "\n".join(code_lines)
        assert "on_failure_callback" not in code_only

    def test_bug17_orphaned_task(self, buggy_dag_source):
        """BUG 17: cleanup task is not in the dependency chain."""
        assert "cleanup" in buggy_dag_source
        assert "cleanup" not in buggy_dag_source.split("extract >> transform >> load")[1].split("cleanup =")[0]


# ===========================================================================
# CLEAN DAG — Should pass all DS team checks
# ===========================================================================

class TestCleanDagPasses:
    """Verify the clean DAG follows all DS team standards."""

    def test_modern_import(self, clean_dag_source):
        """Uses modern airflow.operators.python import."""
        assert "from airflow.operators.python import PythonOperator" in clean_dag_source

    def test_no_unused_imports(self, clean_dag_source):
        """No unused imports present."""
        assert "import os" not in clean_dag_source
        assert "import pandas" not in clean_dag_source

    def test_default_args_complete(self, clean_dag_source):
        """default_args has owner, start_date, retries, retry_delay."""
        keys = get_default_args_keys(clean_dag_source)
        assert "owner" in keys
        assert "start_date" in keys
        assert "retries" in keys
        assert "retry_delay" in keys

    def test_catchup_false(self, clean_dag_source):
        """catchup is explicitly set to False."""
        assert "catchup=False" in clean_dag_source

    def test_snake_case_dag_id(self, clean_dag_source):
        """DAG ID uses snake_case."""
        match = re.search(r"dag_id=['\"](\w+)['\"]", clean_dag_source)
        assert match is not None
        dag_id = match.group(1)
        assert dag_id == dag_id.lower()
        assert "_" in dag_id

    def test_no_hardcoded_credentials(self, clean_dag_source):
        """No passwords or secrets in source."""
        assert "password" not in clean_dag_source.lower()
        assert "secret" not in clean_dag_source.lower()

    def test_uses_airflow_connection(self, clean_dag_source):
        """Uses Airflow Connection for Snowflake, not hardcoded."""
        assert "snowflake_conn_id" in clean_dag_source

    def test_on_failure_callback_present(self, clean_dag_source):
        """on_failure_callback is configured."""
        assert "on_failure_callback" in clean_dag_source

    def test_no_provide_context(self, clean_dag_source):
        """Does not use deprecated provide_context."""
        assert "provide_context" not in clean_dag_source

    def test_no_orphaned_tasks(self, clean_dag_source):
        """All tasks are connected via dependency chain."""
        assert "extract >> transform >> validate" in clean_dag_source

    def test_fully_qualified_table_names(self, clean_dag_source):
        """SQL uses fully qualified names (FIL.STORE)."""
        assert "FIL.STORE" in clean_dag_source
        assert "FIL.STORE_METRICS_DAILY" in clean_dag_source

    def test_divide_by_zero_protection(self, clean_dag_source):
        """Division protected by CASE WHEN > 0."""
        assert "WHEN s.total_orders > 0" in clean_dag_source

    def test_null_handling_with_coalesce(self, clean_dag_source):
        """Uses COALESCE for nullable fields."""
        assert "COALESCE" in clean_dag_source

    def test_no_select_star(self, clean_dag_source):
        """Does not use SELECT *."""
        sql_sections = re.findall(r'sql="""\s*(.*?)"""', clean_dag_source, re.DOTALL)
        for sql in sql_sections:
            assert "SELECT *" not in sql

    def test_no_todo_comments(self, clean_dag_source):
        """No TODO/FIXME left in code."""
        assert "TODO" not in clean_dag_source
        assert "FIXME" not in clean_dag_source

    def test_jira_ticket_in_docstring(self, clean_dag_source):
        """Docstring references a Jira ticket."""
        assert "DS-4521" in clean_dag_source

    def test_has_dag_tags(self, clean_dag_source):
        """DAG has tags for discoverability."""
        assert "tags=" in clean_dag_source
