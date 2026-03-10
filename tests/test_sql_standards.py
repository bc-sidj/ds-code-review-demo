"""Tests for SQL/Snowflake DDL/DML standards.

Validates that:
- The buggy SQL contains all 13 known issues
- The clean SQL passes all DS team checks
"""

import re
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_unqualified_tables(sql: str, known_tables: list[str]) -> list[str]:
    """Find table names referenced without schema prefix."""
    unqualified = []
    for table in known_tables:
        pattern = rf"(?<!\w\.)\b{table}\b"
        qualified_pattern = rf"\w+\.{table}\b"
        if re.search(pattern, sql, re.IGNORECASE):
            qualified_matches = re.findall(qualified_pattern, sql, re.IGNORECASE)
            unqualified_matches = re.findall(pattern, sql, re.IGNORECASE)
            if len(unqualified_matches) > len(qualified_matches):
                unqualified.append(table)
    return unqualified


# ===========================================================================
# BUGGY SQL — Should contain all 13 issues
# ===========================================================================

class TestBuggySqlIssues:
    """Verify the buggy SQL contains all 18 intentional issues."""

    def test_bug01_missing_use_database(self, buggy_sql_source):
        """BUG 1: No USE DATABASE FUJI statement."""
        code_lines = [l for l in buggy_sql_source.splitlines() if not l.strip().startswith("--")]
        code_only = "\n".join(code_lines)
        assert "USE DATABASE" not in code_only.upper()

    def test_bug02_has_use_schema(self, buggy_sql_source):
        """BUG 2: Contains USE SCHEMA (forbidden in DDL files)."""
        assert "USE SCHEMA" in buggy_sql_source.upper()

    def test_bug03_missing_object_comment(self, buggy_sql_source):
        """BUG 3: No COMMENT with Jira ticket on created object."""
        assert "COMMENT" not in buggy_sql_source.upper().split("CREATE")[1].split("SELECT")[0]

    def test_bug04_view_not_fully_qualified(self, buggy_sql_source):
        """BUG 4: View name is not fully qualified (should be vz_apps.vw_store_summary)."""
        match = re.search(r"CREATE OR REPLACE VIEW\s+(\S+)", buggy_sql_source)
        assert match is not None
        view_name = match.group(1)
        assert "." not in view_name

    def test_bug05_select_star_from_subquery(self, buggy_sql_source):
        """BUG 5: Uses sub.* (SELECT * from subquery)."""
        assert "sub.*" in buggy_sql_source

    def test_bug06_divide_by_zero_risk(self, buggy_sql_source):
        """BUG 6: Division without NULLIF or CASE protection."""
        assert "s.total_revenue / s.total_orders" in buggy_sql_source
        code_lines = [l for l in buggy_sql_source.splitlines() if not l.strip().startswith("--")]
        div_line = [l for l in code_lines if "s.total_revenue / s.total_orders" in l][0]
        assert "CASE" not in div_line.upper()
        assert "NULLIF" not in div_line.upper()

    def test_bug07_null_propagation_left_join(self, buggy_sql_source):
        """BUG 7: LEFT JOIN columns without COALESCE."""
        assert "LEFT JOIN" in buggy_sql_source.upper()
        assert "p.plan_name" in buggy_sql_source
        assert "COALESCE(p.plan_name" not in buggy_sql_source

    def test_bug08_unqualified_store_table(self, buggy_sql_source):
        """BUG 8: STORE table not fully qualified."""
        from_section = re.search(r"FROM\s+STORE\s+", buggy_sql_source)
        assert from_section is not None

    def test_bug09_unqualified_plan_table(self, buggy_sql_source):
        """BUG 9: plan table not fully qualified."""
        join_section = re.search(r"JOIN\s+plan\s+", buggy_sql_source, re.IGNORECASE)
        assert join_section is not None

    def test_bug10_unqualified_orders_table(self, buggy_sql_source):
        """BUG 10: orders table not fully qualified."""
        orders_ref = re.search(r"FROM\s+orders\b", buggy_sql_source, re.IGNORECASE)
        assert orders_ref is not None
        line = buggy_sql_source[orders_ref.start()-20:orders_ref.end()]
        assert "FIL.orders" not in line and "fil.orders" not in line

    def test_bug11_subquery_missing_where(self, buggy_sql_source):
        """BUG 11: Subquery on orders table has no WHERE clause."""
        code_lines = [l for l in buggy_sql_source.splitlines() if not l.strip().startswith("--")]
        code_only = "\n".join(code_lines)
        subquery = re.search(
            r"SELECT\s+store_id.*?GROUP BY",
            code_only,
            re.DOTALL | re.IGNORECASE,
        )
        assert subquery is not None
        assert "WHERE" not in subquery.group().upper()

    def test_bug12_inner_join_drops_zero_order_stores(self, buggy_sql_source):
        """BUG 12: Uses INNER JOIN for orders subquery, dropping stores with no orders."""
        assert "INNER JOIN" in buggy_sql_source.upper()

    def test_bug13_update_without_where(self, buggy_sql_source):
        """BUG 13: UPDATE statement has no WHERE clause — CRITICAL."""
        update_section = buggy_sql_source.split("UPDATE")[1] if "UPDATE" in buggy_sql_source else ""
        assert "WHERE" not in update_section.upper().split(";")[0]

    def test_bug14_missing_sp_rollout(self, buggy_sql_source):
        """BUG 14: No sp_rollout wrapping for rollout SQL."""
        assert "sp_rollout" not in buggy_sql_source

    def test_bug15_no_backup_before_change(self, buggy_sql_source):
        """BUG 15: No CLONE backup before destructive change."""
        assert "CLONE" not in buggy_sql_source.upper()

    def test_bug16_nondeterministic_row_number(self, buggy_sql_source):
        """BUG 16: ROW_NUMBER ORDER BY lacks tiebreaker — ambiguity risk."""
        assert "ROW_NUMBER()" in buggy_sql_source.upper()
        # The ORDER BY amount alone is non-deterministic when amounts are equal
        rn_section = buggy_sql_source.upper().split("ROW_NUMBER()")[1].split("AS RN")[0]
        assert "ORDER BY AMOUNT" in rn_section
        assert "ORDER_ID" not in rn_section  # No tiebreaker column

    def test_bug17_varchar_too_small(self, buggy_sql_source):
        """BUG 17: VARCHAR(50) is too small for store descriptions."""
        assert "VARCHAR(50)" in buggy_sql_source.upper()

    def test_bug18_no_downstream_check(self, buggy_sql_source):
        """BUG 18: No downstream dependency check documented."""
        assert "security.table_usage_summary" not in buggy_sql_source


# ===========================================================================
# CLEAN SQL — Should pass all DS team checks
# ===========================================================================

class TestCleanSqlPasses:
    """Verify the clean SQL follows all DS team standards."""

    def test_has_use_database(self, clean_sql_source):
        """USE DATABASE FUJI is present."""
        assert "USE DATABASE FUJI" in clean_sql_source.upper()

    def test_no_use_schema(self, clean_sql_source):
        """No USE SCHEMA statement."""
        assert "USE SCHEMA" not in clean_sql_source.upper()

    def test_view_fully_qualified(self, clean_sql_source):
        """View name is fully qualified with schema."""
        match = re.search(r"CREATE OR REPLACE VIEW\s+(\S+)", clean_sql_source)
        assert match is not None
        view_name = match.group(1)
        assert "." in view_name

    def test_object_comment_with_ticket(self, clean_sql_source):
        """Object has COMMENT with DS ticket number."""
        assert "COMMENT" in clean_sql_source.upper()
        assert re.search(r"DS-\d{4}", clean_sql_source) is not None

    def test_all_tables_fully_qualified(self, clean_sql_source):
        """All table references use schema prefix (FIL.)."""
        assert "FIL.STORE" in clean_sql_source.upper()
        assert "FIL.PLAN" in clean_sql_source.upper()
        assert "FIL.ORDERS" in clean_sql_source.upper()

    def test_no_select_star(self, clean_sql_source):
        """No SELECT * or alias.* used."""
        assert "SELECT *" not in clean_sql_source.upper()
        assert re.search(r"\w+\.\*", clean_sql_source) is None

    def test_divide_by_zero_protection(self, clean_sql_source):
        """Division is protected by CASE WHEN > 0."""
        assert "WHEN COALESCE(sub.order_count, 0) > 0" in clean_sql_source

    def test_null_handling_with_coalesce(self, clean_sql_source):
        """LEFT JOIN nullable columns wrapped in COALESCE."""
        assert "COALESCE(sub.order_count, 0)" in clean_sql_source
        assert "COALESCE(sub.total_amount, 0)" in clean_sql_source
        assert "COALESCE(p.plan_name," in clean_sql_source
        assert "COALESCE(p.plan_tier," in clean_sql_source

    def test_left_join_for_optional_data(self, clean_sql_source):
        """Uses LEFT JOIN (not INNER) so stores with no orders are kept."""
        join_matches = re.findall(r"(LEFT|INNER)\s+JOIN", clean_sql_source.upper())
        assert all(j == "LEFT" for j in join_matches)

    def test_subquery_has_where_clause(self, clean_sql_source):
        """Orders subquery has a WHERE clause for date filtering."""
        subquery = re.search(
            r"SELECT\s+store_id.*?GROUP BY",
            clean_sql_source,
            re.DOTALL | re.IGNORECASE,
        )
        assert subquery is not None
        assert "WHERE" in subquery.group().upper()

    def test_has_active_filter(self, clean_sql_source):
        """Filters on is_active = TRUE."""
        assert "s.is_active = TRUE" in clean_sql_source

    def test_no_update_without_where(self, clean_sql_source):
        """No UPDATE/DELETE without WHERE clause."""
        for keyword in ("UPDATE", "DELETE"):
            if keyword in clean_sql_source.upper():
                stmt = clean_sql_source.upper().split(keyword)[1].split(";")[0]
                assert "WHERE" in stmt

    def test_has_sp_rollout_wrapping(self, clean_sql_source):
        """Rollout SQL wrapped with sp_rollout start and end."""
        assert "sp_rollout('start'" in clean_sql_source
        assert "sp_rollout('end'" in clean_sql_source

    def test_has_backup_clone(self, clean_sql_source):
        """Backup created via CLONE before destructive changes."""
        assert "CLONE" in clean_sql_source.upper()

    def test_clone_has_drop_date_comment(self, clean_sql_source):
        """Backup CLONE has a comment with drop date."""
        assert "Drop after" in clean_sql_source

    def test_deterministic_row_number(self, clean_sql_source):
        """ROW_NUMBER has deterministic ORDER BY with tiebreaker."""
        assert "order by amount desc, order_id desc" in clean_sql_source.lower()

    def test_adequate_varchar_length(self, clean_sql_source):
        """VARCHAR length is adequate (not truncation-prone)."""
        assert "VARCHAR(500)" in clean_sql_source.upper()

    def test_downstream_check_documented(self, clean_sql_source):
        """Downstream dependency check is documented."""
        assert "security.table_usage_summary" in clean_sql_source

    def test_role_returns_to_dev(self, clean_sql_source):
        """Rollout ends with dev role switch."""
        assert "FUJI_DEV_OWNER" in clean_sql_source
