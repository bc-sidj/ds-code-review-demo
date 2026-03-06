"""Edge case tests for DS code review demo files.

Validates handling of:
- Empty datasets / zero rows
- NULL values in key fields
- Divide-by-zero in calculations
- NULL propagation in LEFT JOINs
- Duplicate handling
- Date boundaries
- Large volume scenarios
"""

import re
import pytest


# ===========================================================================
# Python DAG — Edge case detection
# ===========================================================================

class TestBuggyDagEdgeCases:
    """Verify the buggy DAG fails to handle edge cases."""

    def test_no_empty_dataset_handling(self, buggy_dag_source):
        """Extract function does not check for empty result set."""
        extract_func = buggy_dag_source.split("def extract_store_data")[1].split("\ndef ")[0]
        assert "fetchall()" in extract_func
        assert "if not results" not in extract_func
        assert "len(results)" not in extract_func

    def test_no_null_check_before_iteration(self, buggy_dag_source):
        """Transform function iterates without checking for None."""
        transform_func = buggy_dag_source.split("def transform_store_data")[1].split("\ndef ")[0]
        assert "for row in data" in transform_func
        assert "if data" not in transform_func

    def test_no_divide_by_zero_guard(self, buggy_dag_source):
        """Division by cost with no zero check."""
        assert "revenue / cost" in buggy_dag_source
        transform_func = buggy_dag_source.split("def transform_store_data")[1].split("\ndef ")[0]
        code_lines = [l for l in transform_func.splitlines() if not l.strip().startswith("#")]
        code_only = "\n".join(code_lines)
        assert "cost == 0" not in code_only
        assert "cost != 0" not in code_only
        assert "cost > 0" not in code_only

    def test_no_xcom_size_guard(self, buggy_dag_source):
        """Pushes unbounded data to XCom with no size check."""
        assert "xcom_push(key='store_data', value=results)" in buggy_dag_source

    def test_no_upstream_failure_handling(self, buggy_dag_source):
        """Transform does not handle missing XCom from failed extract."""
        transform_func = buggy_dag_source.split("def transform_store_data")[1].split("\ndef ")[0]
        assert "xcom_pull" in transform_func
        assert "is None" not in transform_func


class TestCleanDagEdgeCases:
    """Verify the clean DAG properly handles edge cases."""

    def test_divide_by_zero_protected(self, clean_dag_source):
        """Uses CASE WHEN total_orders > 0 for division."""
        assert "WHEN s.total_orders > 0" in clean_dag_source
        assert "THEN s.total_revenue / s.total_orders" in clean_dag_source
        assert "ELSE 0" in clean_dag_source

    def test_null_values_coalesced(self, clean_dag_source):
        """COALESCE wraps nullable fields."""
        assert "COALESCE(s.total_orders, 0)" in clean_dag_source
        assert "COALESCE(s.total_revenue, 0)" in clean_dag_source

    def test_validation_step_catches_empty_load(self, clean_dag_source):
        """Validate task fails if zero rows were loaded."""
        assert "WHEN COUNT(*) = 0 THEN 1/0" in clean_dag_source

    def test_date_filtering_uses_template(self, clean_dag_source):
        """Uses Airflow template {{ ds }} for date boundaries."""
        assert "{{ ds }}" in clean_dag_source

    def test_retry_configured(self, clean_dag_source):
        """Retries are set for transient failures."""
        assert "'retries': 2" in clean_dag_source
        assert "retry_delay" in clean_dag_source


# ===========================================================================
# SQL — Edge case detection
# ===========================================================================

class TestBuggySqlEdgeCases:
    """Verify the buggy SQL fails to handle edge cases."""

    def test_divide_by_zero_unprotected(self, buggy_sql_source):
        """Division without CASE or NULLIF."""
        assert "s.total_revenue / s.total_orders" in buggy_sql_source
        code_lines = [l for l in buggy_sql_source.splitlines() if not l.strip().startswith("--")]
        div_line = [l for l in code_lines if "s.total_revenue / s.total_orders" in l][0]
        assert "CASE" not in div_line.upper()
        assert "NULLIF" not in div_line.upper()

    def test_null_propagation_from_left_join(self, buggy_sql_source):
        """LEFT JOIN columns used directly without COALESCE."""
        assert "p.plan_name" in buggy_sql_source
        assert "COALESCE(p.plan_name" not in buggy_sql_source

    def test_inner_join_drops_rows(self, buggy_sql_source):
        """INNER JOIN on orders silently drops stores with no orders."""
        assert re.search(r"INNER\s+JOIN\s*\(", buggy_sql_source, re.IGNORECASE)

    def test_full_table_scan_on_orders(self, buggy_sql_source):
        """Orders subquery has no WHERE clause — scans entire table."""
        code_lines = [l for l in buggy_sql_source.splitlines() if not l.strip().startswith("--")]
        code_only = "\n".join(code_lines)
        subquery = re.search(
            r"SELECT\s+store_id.*?GROUP BY",
            code_only,
            re.DOTALL | re.IGNORECASE,
        )
        assert "WHERE" not in subquery.group().upper()

    def test_update_affects_all_rows(self, buggy_sql_source):
        """UPDATE has no WHERE clause — modifies every row."""
        update_section = buggy_sql_source.split("UPDATE")[1].split(";")[0]
        assert "WHERE" not in update_section.upper()

    def test_no_duplicate_handling(self, buggy_sql_source):
        """No DISTINCT or deduplication logic present."""
        assert "DISTINCT" not in buggy_sql_source.upper()
        assert "ROW_NUMBER()" not in buggy_sql_source.upper()
        assert "QUALIFY" not in buggy_sql_source.upper()


class TestCleanSqlEdgeCases:
    """Verify the clean SQL properly handles edge cases."""

    def test_divide_by_zero_protected(self, clean_sql_source):
        """Division protected by CASE WHEN > 0."""
        assert "WHEN COALESCE(sub.order_count, 0) > 0" in clean_sql_source
        assert "ELSE 0" in clean_sql_source

    def test_null_coalesced_from_joins(self, clean_sql_source):
        """All LEFT JOIN nullable columns use COALESCE with defaults."""
        assert "COALESCE(p.plan_name, 'Unknown')" in clean_sql_source
        assert "COALESCE(p.plan_tier, 'Unknown')" in clean_sql_source
        assert "COALESCE(sub.order_count, 0)" in clean_sql_source
        assert "COALESCE(sub.total_amount, 0)" in clean_sql_source

    def test_left_join_preserves_all_stores(self, clean_sql_source):
        """Uses LEFT JOIN so stores with no orders or plans are kept."""
        joins = re.findall(r"(LEFT|INNER)\s+JOIN", clean_sql_source.upper())
        assert all(j == "LEFT" for j in joins)

    def test_date_bounded_query(self, clean_sql_source):
        """Orders subquery filters by date to limit scan."""
        assert "DATEADD('year', -1, CURRENT_DATE())" in clean_sql_source

    def test_active_store_filter(self, clean_sql_source):
        """Only processes active stores."""
        assert "s.is_active = TRUE" in clean_sql_source


# ===========================================================================
# Repo structure and config
# ===========================================================================

class TestRepoStructure:
    """Verify repo has all required files and configuration."""

    def test_context_md_exists(self):
        """CONTEXT.md exists at repo root."""
        from conftest import ROOT
        assert (ROOT / "CONTEXT.md").exists()

    def test_review_instructions_exist(self):
        """Review instructions doc exists."""
        from conftest import ROOT
        assert (ROOT / "docs" / "code-review-instructions.md").exists()

    def test_workflow_exists(self):
        """GitHub Actions workflow exists."""
        from conftest import ROOT
        assert (ROOT / ".github" / "workflows" / "code-review.yml").exists()

    def test_pr_description_workflow_exists(self):
        """PR description workflow exists."""
        from conftest import ROOT
        assert (ROOT / ".github" / "workflows" / "pr-description.yml").exists()

    def test_review_script_exists(self):
        """Review Python script exists."""
        from conftest import ROOT
        assert (ROOT / ".github" / "scripts" / "review.py").exists()

    def test_pr_desc_script_exists(self):
        """PR description Python script exists."""
        from conftest import ROOT
        assert (ROOT / ".github" / "scripts" / "generate_pr_description.py").exists()

    def test_gitignore_has_code_reviews(self):
        """code_reviews/*.md is gitignored."""
        from conftest import ROOT
        gitignore = (ROOT / ".gitignore").read_text()
        assert "code_reviews/" in gitignore

    def test_pr_template_exists(self):
        """PR description template exists."""
        from conftest import ROOT
        assert (ROOT / ".github" / "pull_request_template.md").exists()
