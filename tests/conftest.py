"""Shared fixtures for DS code review test suite."""

import pathlib
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

BUGGY_DAG_PATH = ROOT / "dags" / "buggy" / "dag_store_metrics_buggy.py"
CLEAN_DAG_PATH = ROOT / "dags" / "clean" / "dag_store_metrics_clean.py"
BUGGY_SQL_PATH = ROOT / "ddl" / "fuji" / "vz_apps" / "buggy" / "vw_store_summary_buggy.sql"
CLEAN_SQL_PATH = ROOT / "ddl" / "fuji" / "vz_apps" / "clean" / "vw_store_summary_clean.sql"
REVIEW_SCRIPT_PATH = ROOT / ".github" / "scripts" / "review.py"
PR_DESC_SCRIPT_PATH = ROOT / ".github" / "scripts" / "generate_pr_description.py"


@pytest.fixture
def buggy_dag_source():
    return BUGGY_DAG_PATH.read_text()


@pytest.fixture
def clean_dag_source():
    return CLEAN_DAG_PATH.read_text()


@pytest.fixture
def buggy_sql_source():
    return BUGGY_SQL_PATH.read_text()


@pytest.fixture
def clean_sql_source():
    return CLEAN_SQL_PATH.read_text()


@pytest.fixture
def review_script_source():
    return REVIEW_SCRIPT_PATH.read_text()


@pytest.fixture
def pr_desc_script_source():
    return PR_DESC_SCRIPT_PATH.read_text()
