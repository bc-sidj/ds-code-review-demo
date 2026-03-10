"""Reusable validators for DS code review.

Applies the same checks from the test suite to arbitrary SQL and Python files.
Checks are organized by the 5 DS Testing Categories:
  1. Data Integrity
  2. Schema & DDL Compliance
  3. Regression
  4. Edge Cases
  5. Business Logic & Downstream

Each check returns a CheckResult with name, status, severity, detail, and category.
"""

import ast
import re
from dataclasses import dataclass, field
from typing import Literal


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    status: Literal["PASS", "FAIL", "WARNING"]
    severity: Literal["CRITICAL", "WARNING", "SUGGESTION"]
    detail: str
    category: int  # 1-5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Return SQL with -- line comments removed."""
    return "\n".join(
        line for line in sql.splitlines()
        if not line.strip().startswith("--")
    )


def _find_divisions(code: str) -> list[tuple[str, bool]]:
    """Find lines with division operators. Returns (line, is_inside_case) tuples."""
    results = []
    in_case = False
    case_depth = 0
    for line in code.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped.startswith("#"):
            continue
        upper = stripped.upper()
        # Track CASE/END nesting
        case_depth += upper.count("CASE") - upper.count("END")
        if "CASE" in upper:
            in_case = True
        if re.search(r'\w\s*/\s*\w', stripped):
            results.append((stripped, case_depth > 0 or in_case))
        if case_depth <= 0:
            in_case = False
            case_depth = 0
    return results


def _get_default_args_keys(source: str) -> set[str]:
    """Extract keys from the default_args dict via AST."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
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


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from FROM and JOIN clauses."""
    tables = []
    for match in re.finditer(r'(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)\b', sql, re.IGNORECASE):
        name = match.group(1)
        # Skip keywords and subqueries
        if name.upper() not in ('SELECT', 'WHERE', 'SET', 'VALUES', 'INTO', 'AS'):
            tables.append(name)
    return tables


# ---------------------------------------------------------------------------
# SQL Validators
# ---------------------------------------------------------------------------

def validate_sql_file(content: str, filename: str) -> list[CheckResult]:
    """Run all DS checks on a SQL file, organized by category."""
    results = []
    code_only = _strip_comments(content)
    upper_content = content.upper()
    upper_code = code_only.upper()

    # ===================================================================
    # Category 1 — Data Integrity
    # ===================================================================

    # No SELECT *
    has_select_star = "SELECT *" in upper_code or re.search(r'\w+\.\*', code_only)
    results.append(CheckResult(
        name="Explicit column list (no SELECT *)",
        status="FAIL" if has_select_star else "PASS",
        severity="WARNING",
        detail="Found SELECT * or alias.* — use explicit column names" if has_select_star
               else "All queries use explicit column lists",
        category=1,
    ))

    # Tables fully qualified
    tables = _extract_tables_from_sql(code_only)
    unqualified = [t for t in tables if '.' not in t]
    results.append(CheckResult(
        name="Fully qualified table references",
        status="FAIL" if unqualified else "PASS",
        severity="WARNING",
        detail=f"Unqualified tables: {', '.join(unqualified)}" if unqualified
               else "All table references are schema-qualified",
        category=1,
    ))

    # Data type casting (explicit casts present)
    has_casts = "::" in content or "CAST(" in upper_content
    results.append(CheckResult(
        name="Explicit data type casting",
        status="PASS" if has_casts else "WARNING",
        severity="SUGGESTION",
        detail="Explicit type casts found" if has_casts
               else "No explicit casts — verify data types are correct",
        category=1,
    ))

    # Date filtering present
    has_date_filter = bool(re.search(
        r'WHERE.*(?:DATE|CURRENT_DATE|DATEADD|GETDATE|ds)',
        upper_code, re.DOTALL | re.IGNORECASE
    ))
    results.append(CheckResult(
        name="Date filtering present",
        status="PASS" if has_date_filter else "WARNING",
        severity="SUGGESTION",
        detail="Date-based WHERE clause found" if has_date_filter
               else "No date filtering detected — may scan full table",
        category=1,
    ))

    # ===================================================================
    # Category 2 — Schema & DDL Compliance
    # ===================================================================

    # USE DATABASE present
    has_use_db = "USE DATABASE" in upper_code
    results.append(CheckResult(
        name="USE DATABASE present",
        status="PASS" if has_use_db else "WARNING",
        severity="WARNING",
        detail="USE DATABASE found" if has_use_db
               else "No USE DATABASE — may execute in wrong database context",
        category=2,
    ))

    # No USE SCHEMA
    has_use_schema = "USE SCHEMA" in upper_code
    results.append(CheckResult(
        name="No USE SCHEMA statement",
        status="FAIL" if has_use_schema else "PASS",
        severity="WARNING",
        detail="USE SCHEMA found — use fully qualified names instead" if has_use_schema
               else "No USE SCHEMA statements",
        category=2,
    ))

    # Object COMMENT with Jira ticket (check code, not comments)
    has_comment = "COMMENT" in upper_code
    has_ticket = bool(re.search(r'(?:DS|ANALYTICS)-\d{3,5}', code_only))
    if has_comment and has_ticket:
        ticket = re.search(r'(?:DS|ANALYTICS)-\d{3,5}', code_only).group()
        comment_status, comment_detail = "PASS", f"COMMENT with {ticket} found"
    elif has_comment:
        comment_status, comment_detail = "WARNING", "COMMENT found but no Jira ticket (DS-XXXX)"
    else:
        comment_status, comment_detail = "FAIL", "No COMMENT on created objects"
    results.append(CheckResult(
        name="Object COMMENT with Jira ticket",
        status=comment_status,
        severity="WARNING",
        detail=comment_detail,
        category=2,
    ))

    # View fully qualified
    view_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\S+)', content, re.IGNORECASE)
    if view_match:
        view_name = view_match.group(1)
        view_qualified = "." in view_name
        results.append(CheckResult(
            name="View name fully qualified",
            status="PASS" if view_qualified else "FAIL",
            severity="WARNING",
            detail=f"View: {view_name}" + (" (qualified)" if view_qualified else " (needs schema prefix)"),
            category=2,
        ))

    # sp_rollout wrapping
    has_rollout_start = "sp_rollout('start'" in content or 'sp_rollout("start"' in content
    has_rollout_end = "sp_rollout('end'" in content or 'sp_rollout("end"' in content
    if has_rollout_start and has_rollout_end:
        rollout_status, rollout_detail = "PASS", "sp_rollout start and end bookends found"
    elif has_rollout_start or has_rollout_end:
        rollout_status, rollout_detail = "FAIL", "Only one sp_rollout bookend found — need both start and end"
    else:
        rollout_status, rollout_detail = "WARNING", "No sp_rollout wrapping — required for rollout SQL"
    results.append(CheckResult(
        name="sp_rollout wrapping",
        status=rollout_status,
        severity="WARNING",
        detail=rollout_detail,
        category=2,
    ))

    # CLONE backup
    has_clone = "CLONE" in upper_code
    has_drop_date = "drop after" in content.lower()
    if has_clone and has_drop_date:
        clone_status, clone_detail = "PASS", "CLONE backup with drop date found"
    elif has_clone:
        clone_status, clone_detail = "WARNING", "CLONE found but no drop date in comment"
    else:
        clone_status, clone_detail = "WARNING", "No CLONE backup — consider adding before destructive changes"
    # Only flag as relevant if there are destructive DDL statements
    has_destructive = bool(re.search(r'(?:DROP|ALTER|CREATE\s+OR\s+REPLACE)', upper_code))
    if has_destructive:
        results.append(CheckResult(
            name="Backup CLONE before changes",
            status=clone_status,
            severity="SUGGESTION",
            detail=clone_detail,
            category=2,
        ))

    # VARCHAR length adequacy
    varchar_matches = re.findall(r'VARCHAR\((\d+)\)', upper_code)
    small_varchars = [v for v in varchar_matches if int(v) < 100]
    if small_varchars:
        results.append(CheckResult(
            name="VARCHAR length adequacy",
            status="WARNING",
            severity="SUGGESTION",
            detail=f"Small VARCHAR found: {', '.join('VARCHAR(' + v + ')' for v in small_varchars)} — verify data fits",
            category=2,
        ))

    # Role returns to dev
    if has_rollout_start:
        has_dev_role = "FUJI_DEV_OWNER" in upper_content or "DEV_OWNER" in upper_content
        results.append(CheckResult(
            name="Role returns to dev at end",
            status="PASS" if has_dev_role else "WARNING",
            severity="SUGGESTION",
            detail="Dev role switch found" if has_dev_role
                   else "Rollout should return to dev role at end",
            category=2,
        ))

    # ===================================================================
    # Category 3 — Regression
    # ===================================================================

    # UPDATE/DELETE has WHERE clause
    for keyword in ("UPDATE", "DELETE"):
        if keyword in upper_code:
            stmts = upper_code.split(keyword)[1:]
            for stmt in stmts:
                stmt_body = stmt.split(";")[0]
                has_where = "WHERE" in stmt_body
                results.append(CheckResult(
                    name=f"{keyword} has WHERE clause",
                    status="PASS" if has_where else "FAIL",
                    severity="CRITICAL",
                    detail=f"{keyword} with WHERE clause" if has_where
                           else f"{keyword} without WHERE — updates/deletes ALL rows",
                    category=3,
                ))

    # Schema changes are additive
    has_drop_column = "DROP COLUMN" in upper_code
    if has_drop_column:
        results.append(CheckResult(
            name="Schema changes are additive",
            status="FAIL",
            severity="WARNING",
            detail="DROP COLUMN detected — verify no downstream dependencies",
            category=3,
        ))

    # ===================================================================
    # Category 4 — Edge Cases
    # ===================================================================

    # Divide-by-zero protection
    div_lines = _find_divisions(code_only)
    unprotected_divs = []
    for line, in_case in div_lines:
        upper_line = line.upper()
        if in_case or "CASE" in upper_line or "NULLIF" in upper_line or "> 0" in upper_line:
            continue
        unprotected_divs.append(line.strip()[:60])
    if div_lines:
        results.append(CheckResult(
            name="Divide-by-zero protection",
            status="FAIL" if unprotected_divs else "PASS",
            severity="CRITICAL" if unprotected_divs else "SUGGESTION",
            detail=f"Unprotected division: {unprotected_divs[0]}..." if unprotected_divs
                   else "All divisions are guarded",
            category=4,
        ))

    # COALESCE on LEFT JOIN columns
    if "LEFT JOIN" in upper_code:
        # Find alias from LEFT JOIN
        join_aliases = re.findall(r'LEFT\s+JOIN\s+\S+\s+(\w+)', code_only, re.IGNORECASE)
        missing_coalesce = []
        for alias in join_aliases:
            # Find columns from this alias used in SELECT
            alias_cols = re.findall(rf'{alias}\.(\w+)', code_only, re.IGNORECASE)
            for col in alias_cols:
                if f"COALESCE({alias}.{col}" not in content and f"coalesce({alias}.{col}" not in content:
                    missing_coalesce.append(f"{alias}.{col}")
        if missing_coalesce:
            unique_missing = list(dict.fromkeys(missing_coalesce))[:3]
            results.append(CheckResult(
                name="COALESCE on LEFT JOIN columns",
                status="WARNING",
                severity="WARNING",
                detail=f"Nullable columns without COALESCE: {', '.join(unique_missing)}",
                category=4,
            ))
        else:
            results.append(CheckResult(
                name="COALESCE on LEFT JOIN columns",
                status="PASS",
                severity="WARNING",
                detail="All LEFT JOIN columns wrapped in COALESCE",
                category=4,
            ))

    # ROW_NUMBER determinism
    if "ROW_NUMBER()" in upper_code:
        rn_sections = upper_code.split("ROW_NUMBER()")[1:]
        for rn in rn_sections:
            order_match = re.search(r'ORDER\s+BY\s+(.*?)(?:\)|AS\s)', rn, re.DOTALL)
            if order_match:
                order_cols = order_match.group(1).strip()
                col_count = len([c for c in order_cols.split(",") if c.strip()])
                results.append(CheckResult(
                    name="ROW_NUMBER deterministic ORDER BY",
                    status="PASS" if col_count >= 2 else "WARNING",
                    severity="WARNING",
                    detail=f"ORDER BY has {col_count} column(s)" + (" — add tiebreaker for determinism" if col_count < 2 else " (includes tiebreaker)"),
                    category=4,
                ))

    # Subquery has WHERE
    subqueries = re.findall(
        r'SELECT\s+\w+.*?GROUP\s+BY',
        code_only, re.DOTALL | re.IGNORECASE
    )
    for i, sq in enumerate(subqueries):
        has_sq_where = "WHERE" in sq.upper()
        results.append(CheckResult(
            name=f"Subquery {i+1} has WHERE clause",
            status="PASS" if has_sq_where else "WARNING",
            severity="WARNING",
            detail="Subquery is date-bounded" if has_sq_where
                   else "Subquery has no WHERE — may cause full table scan",
            category=4,
        ))

    # ===================================================================
    # Category 5 — Business Logic & Downstream
    # ===================================================================

    # Downstream dependency documentation
    has_downstream = (
        "table_usage_summary" in content.lower()
        or "downstream" in content.lower()
    )
    results.append(CheckResult(
        name="Downstream dependency documented",
        status="PASS" if has_downstream else "WARNING",
        severity="SUGGESTION",
        detail="Downstream dependency check documented" if has_downstream
               else "No downstream dependency documentation — verify no AIRFLOW/TABLEAU consumers",
        category=5,
    ))

    return results


# ---------------------------------------------------------------------------
# DAG / Python Validators
# ---------------------------------------------------------------------------

def validate_dag_file(content: str, filename: str) -> list[CheckResult]:
    """Run all DS checks on a Python/Airflow DAG file, organized by category."""
    results = []
    code_lines = [l for l in content.splitlines() if not l.strip().startswith("#")]
    code_only = "\n".join(code_lines)

    # ===================================================================
    # Category 1 — Data Integrity
    # ===================================================================

    # Explicit columns in embedded SQL (triple-quoted and single-line strings)
    raw_blocks = re.findall(r'(?:sql=)?"""(.*?)"""', content, re.DOTALL)
    raw_blocks += re.findall(r"(?:sql=)?'''(.*?)'''", content, re.DOTALL)
    # Filter to blocks that contain actual SQL statements (exclude docstrings)
    sql_keywords = ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')
    sql_blocks = [b for b in raw_blocks if any(kw in b.upper() for kw in sql_keywords)]
    # Also capture single-line SQL strings (e.g. cursor.execute("SELECT ..."))
    single_line_sql = re.findall(r'(?:execute|sql)\s*\(\s*["\']([^"\']+)["\']', code_only)
    all_sql_blocks = sql_blocks + single_line_sql
    has_select_star_sql = any("SELECT *" in block.upper() for block in all_sql_blocks)
    if all_sql_blocks:
        results.append(CheckResult(
            name="No SELECT * in embedded SQL",
            status="FAIL" if has_select_star_sql else "PASS",
            severity="WARNING",
            detail="SELECT * found in embedded SQL" if has_select_star_sql
                   else "Embedded SQL uses explicit column lists",
            category=1,
        ))

    # Fully qualified tables in SQL
    for block in sql_blocks:
        tables = _extract_tables_from_sql(block)
        unqualified = [t for t in tables if '.' not in t]
        if tables:
            results.append(CheckResult(
                name="Fully qualified tables in SQL",
                status="FAIL" if unqualified else "PASS",
                severity="WARNING",
                detail=f"Unqualified: {', '.join(unqualified)}" if unqualified
                       else "All SQL tables are schema-qualified",
                category=1,
            ))

    # ===================================================================
    # Category 2 — Schema & DDL Compliance
    # ===================================================================

    # Modern imports
    has_deprecated = "from airflow.operators.python_operator" in content
    has_modern = "from airflow.operators.python" in content
    results.append(CheckResult(
        name="Modern Airflow imports",
        status="FAIL" if has_deprecated else "PASS",
        severity="WARNING",
        detail="Uses deprecated airflow.operators.python_operator" if has_deprecated
               else "Uses modern import paths",
        category=2,
    ))

    # default_args completeness
    da_keys = _get_default_args_keys(content)
    if da_keys is not None and len(da_keys) > 0:
        required = {"owner", "retries", "retry_delay"}
        missing = required - da_keys
        results.append(CheckResult(
            name="default_args complete",
            status="FAIL" if missing else "PASS",
            severity="WARNING",
            detail=f"Missing: {', '.join(sorted(missing))}" if missing
                   else f"Has {', '.join(sorted(da_keys & required))}",
            category=2,
        ))

        # on_failure_callback (check code, not comments)
        has_callback = "on_failure_callback" in code_only
        results.append(CheckResult(
            name="on_failure_callback present",
            status="PASS" if has_callback else "FAIL",
            severity="WARNING",
            detail="Failure callback configured" if has_callback
                   else "No on_failure_callback — task failures won't trigger alerts",
            category=2,
        ))

    # catchup explicitly set
    has_catchup = "catchup=" in code_only or "catchup =" in code_only
    results.append(CheckResult(
        name="catchup explicitly set",
        status="PASS" if has_catchup else "FAIL",
        severity="WARNING",
        detail="catchup is explicitly configured" if has_catchup
               else "catchup not set — defaults to True (will backfill all dates)",
        category=2,
    ))

    # snake_case DAG ID (match both dag_id='x' and DAG('x'))
    dag_id_match = re.search(r"dag_id=['\"](\w+)['\"]", content)
    if not dag_id_match:
        dag_id_match = re.search(r"DAG\(\s*['\"](\w+)['\"]", content)
    if dag_id_match:
        dag_id = dag_id_match.group(1)
        is_snake = dag_id == dag_id.lower()
        results.append(CheckResult(
            name="snake_case DAG ID",
            status="PASS" if is_snake else "FAIL",
            severity="SUGGESTION",
            detail=f"DAG ID: {dag_id}" + ("" if is_snake else " — should be snake_case"),
            category=2,
        ))

    # No deprecated provide_context
    has_provide_ctx = "provide_context" in content
    if has_provide_ctx:
        results.append(CheckResult(
            name="No deprecated provide_context",
            status="FAIL",
            severity="SUGGESTION",
            detail="provide_context=True is deprecated in Airflow 2.x",
            category=2,
        ))

    # No TODO/FIXME
    has_todo = "TODO" in content or "FIXME" in content
    results.append(CheckResult(
        name="No TODO/FIXME comments",
        status="FAIL" if has_todo else "PASS",
        severity="SUGGESTION",
        detail="TODO/FIXME found — resolve before merging" if has_todo
               else "No TODO/FIXME comments",
        category=2,
    ))

    # Jira ticket referenced
    has_ticket = bool(re.search(r'(?:DS|ANALYTICS)-\d{3,5}', content))
    results.append(CheckResult(
        name="Jira ticket referenced",
        status="PASS" if has_ticket else "WARNING",
        severity="SUGGESTION",
        detail=re.search(r'(?:DS|ANALYTICS)-\d{3,5}', content).group() + " found" if has_ticket
               else "No Jira ticket reference (DS-XXXX) in code or docstring",
        category=2,
    ))

    # DAG tags
    has_tags = "tags=" in content or "tags =" in content
    results.append(CheckResult(
        name="DAG tags present",
        status="PASS" if has_tags else "WARNING",
        severity="SUGGESTION",
        detail="DAG has tags for discoverability" if has_tags
               else "No tags= on DAG — add for discoverability",
        category=2,
    ))

    # ===================================================================
    # Category 3 — Regression
    # ===================================================================

    # Unused imports
    import_lines = [l.strip() for l in content.splitlines() if l.strip().startswith(("import ", "from "))]
    unused = []
    for imp in import_lines:
        # Extract the imported name
        if imp.startswith("from "):
            parts = imp.split("import")
            if len(parts) >= 2:
                names = [n.strip().split(" as ")[-1].strip() for n in parts[1].split(",")]
                for name in names:
                    if name and content.count(name) <= 1:
                        unused.append(name)
        elif imp.startswith("import "):
            name = imp.replace("import ", "").strip().split(" as ")[-1].strip()
            if name and content.count(name) <= 1:
                unused.append(name)
    results.append(CheckResult(
        name="No unused imports",
        status="FAIL" if unused else "PASS",
        severity="WARNING",
        detail=f"Unused: {', '.join(unused)}" if unused
               else "All imports are used",
        category=3,
    ))

    # ===================================================================
    # Category 4 — Edge Cases
    # ===================================================================

    # No hardcoded credentials
    cred_patterns = [
        r'password\s*=\s*["\']',
        r'secret\s*=\s*["\']',
        r'P@ss',
        r'api_key\s*=\s*["\'](?!{)',
        r'token\s*=\s*["\'][A-Za-z0-9]',
    ]
    cred_found = any(re.search(p, content, re.IGNORECASE) for p in cred_patterns)
    results.append(CheckResult(
        name="No hardcoded credentials",
        status="FAIL" if cred_found else "PASS",
        severity="CRITICAL",
        detail="Hardcoded credentials detected — use Airflow Connections/Variables" if cred_found
               else "No hardcoded credential patterns found",
        category=4,
    ))

    # No hardcoded paths
    path_patterns = [r'/home/', r'/opt/airflow/', r'/tmp/data/', r'/var/']
    path_found = any(re.search(p, code_only) for p in path_patterns)
    if path_found:
        results.append(CheckResult(
            name="No hardcoded paths",
            status="FAIL",
            severity="WARNING",
            detail="Hardcoded filesystem path detected — use Variables or config",
            category=4,
        ))

    # Uses Airflow Connection (not hardcoded connection strings)
    has_conn_id = "conn_id" in content
    has_connector = "snowflake.connector.connect" in content or "connect(" in code_only
    if has_connector and not has_conn_id:
        results.append(CheckResult(
            name="Uses Airflow Connection",
            status="FAIL",
            severity="WARNING",
            detail="Direct connector used — prefer Airflow Connection (conn_id)",
            category=4,
        ))
    elif has_conn_id:
        results.append(CheckResult(
            name="Uses Airflow Connection",
            status="PASS",
            severity="WARNING",
            detail="Uses conn_id for database connections",
            category=4,
        ))

    # Divide-by-zero in embedded SQL
    for block in sql_blocks:
        div_lines = _find_divisions(block)
        for line, in_case in div_lines:
            upper_line = line.upper()
            if in_case or "CASE" in upper_line or "NULLIF" in upper_line or "> 0" in upper_line:
                continue
            results.append(CheckResult(
                name="Divide-by-zero in SQL",
                status="FAIL",
                severity="CRITICAL",
                detail=f"Unprotected: {line.strip()[:60]}",
                category=4,
            ))
            break

    # ===================================================================
    # Category 5 — Business Logic & Downstream
    # ===================================================================

    # Task chain complete (no orphaned tasks)
    task_assigns = re.findall(r'^\s*(\w+)\s*=\s*\w*Operator\(', content, re.MULTILINE)
    chained_tasks = set(re.findall(r'(\w+)\s*>>', content))
    chained_tasks.update(re.findall(r'>>\s*(\w+)', content))
    # Also check list notation [a, b]
    list_chains = re.findall(r'\[([^\]]+)\]', content)
    for lc in list_chains:
        for name in re.findall(r'(\w+)', lc):
            if name in task_assigns:
                chained_tasks.add(name)
    # Also check chain_linear args
    chain_args = re.findall(r'chain_linear\((.*?)\)', content, re.DOTALL)
    for ca in chain_args:
        for name in re.findall(r'(\w+)', ca):
            if name in task_assigns:
                chained_tasks.add(name)

    orphaned = [t for t in task_assigns if t not in chained_tasks and t not in ('dag',)]
    if task_assigns:
        results.append(CheckResult(
            name="All tasks in dependency chain",
            status="FAIL" if orphaned else "PASS",
            severity="WARNING",
            detail=f"Orphaned tasks: {', '.join(orphaned)}" if orphaned
                   else "All tasks are connected",
            category=5,
        ))

    # DAG tags for downstream discoverability
    if has_tags:
        results.append(CheckResult(
            name="Export/pipeline tagging",
            status="PASS",
            severity="SUGGESTION",
            detail="DAG is tagged for downstream discoverability",
            category=5,
        ))

    # Downstream documentation
    has_downstream = (
        "table_usage_summary" in content.lower()
        or "downstream" in content.lower()
        or "confluence" in content.lower()
        or "doc_md" in content
    )
    results.append(CheckResult(
        name="Downstream documentation",
        status="PASS" if has_downstream else "WARNING",
        severity="SUGGESTION",
        detail="Documentation reference found" if has_downstream
               else "No downstream dependency documentation",
        category=5,
    ))

    return results


# ---------------------------------------------------------------------------
# Diff parser
# ---------------------------------------------------------------------------

def parse_diff_files(diff_text: str) -> list[dict]:
    """Parse git diff output to extract changed file paths and status."""
    files = []
    seen = set()

    for match in re.finditer(r'^diff --git a/(.*?) b/(.*?)$', diff_text, re.MULTILINE):
        path = match.group(2)
        if path in seen:
            continue
        seen.add(path)

        # Determine extension
        ext = ""
        if "." in path:
            ext = "." + path.rsplit(".", 1)[1]

        # Determine status
        # Look for the section after this diff header
        start = match.end()
        next_diff = diff_text.find("diff --git", start)
        section = diff_text[start:next_diff] if next_diff != -1 else diff_text[start:]

        if "deleted file" in section:
            status = "deleted"
        elif "new file" in section:
            status = "added"
        elif "rename from" in section:
            status = "renamed"
        elif "Binary files" in section:
            status = "binary"
        else:
            status = "modified"

        files.append({
            "path": path,
            "extension": ext,
            "status": status,
        })

    return files


# ---------------------------------------------------------------------------
# Category labels
# ---------------------------------------------------------------------------

CATEGORY_NAMES = {
    1: "Data Integrity",
    2: "Schema & DDL Compliance",
    3: "Regression",
    4: "Edge Cases",
    5: "Business Logic & Downstream",
}
