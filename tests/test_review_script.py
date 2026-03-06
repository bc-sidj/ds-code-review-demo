"""Tests for the review.py and generate_pr_description.py scripts.

Validates script structure, prompt content, and error handling
without calling any external APIs.
"""

import ast
import re
import pytest

from conftest import REVIEW_SCRIPT_PATH, PR_DESC_SCRIPT_PATH


# ===========================================================================
# review.py
# ===========================================================================

class TestReviewScript:
    """Validate the automated code review script."""

    def test_script_parses_as_valid_python(self, review_script_source):
        """Script is syntactically valid Python."""
        ast.parse(review_script_source)

    def test_uses_openai_client(self, review_script_source):
        """Uses the openai package for API calls."""
        assert "from openai import OpenAI" in review_script_source

    def test_reads_diff_from_tmp(self, review_script_source):
        """Reads the git diff from /tmp/diff.txt."""
        assert "/tmp/diff.txt" in review_script_source

    def test_reads_context_md(self, review_script_source):
        """Loads project context from CONTEXT.md."""
        assert "CONTEXT.md" in review_script_source

    def test_has_max_diff_limit(self, review_script_source):
        """Truncates large diffs to prevent token overflow."""
        assert "MAX_DIFF_CHARS" in review_script_source

    def test_handles_empty_diff(self, review_script_source):
        """Returns a safe message when diff is empty."""
        assert "No changes detected" in review_script_source

    def test_handles_missing_diff_file(self, review_script_source):
        """Exits with error when diff file is missing."""
        assert "sys.exit(1)" in review_script_source

    def test_supports_multiple_api_keys(self, review_script_source):
        """Checks OPENAI_API_KEY, OPENROUTER_API_KEY, and ANTHROPIC_API_KEY."""
        assert "OPENAI_API_KEY" in review_script_source
        assert "OPENROUTER_API_KEY" in review_script_source
        assert "ANTHROPIC_API_KEY" in review_script_source

    def test_prompt_covers_ds_workflow_compliance(self, review_script_source):
        """Review prompt checks for Jira ticket in commits."""
        assert "Jira ticket" in review_script_source

    def test_prompt_covers_python_checks(self, review_script_source):
        """Review prompt covers Python/Airflow standards."""
        assert "catchup" in review_script_source
        assert "default_args" in review_script_source
        assert "on_failure_callback" in review_script_source
        assert "hardcoded credentials" in review_script_source

    def test_prompt_covers_sql_checks(self, review_script_source):
        """Review prompt covers SQL/Snowflake standards."""
        assert "fully qualified" in review_script_source
        assert "USE SCHEMA" in review_script_source
        assert "WHERE clause" in review_script_source
        assert "COMMENT" in review_script_source

    def test_prompt_includes_severity_levels(self, review_script_source):
        """Review prompt uses all four severity levels."""
        assert "CRITICAL" in review_script_source
        assert "WARNING" in review_script_source
        assert "SUGGESTION" in review_script_source
        assert "PASS" in review_script_source

    def test_prompt_includes_sox_reminder(self, review_script_source):
        """Review prompt includes SOX compliance reminder."""
        assert "SOX" in review_script_source

    def test_prompt_requests_test_cases(self, review_script_source):
        """Review prompt asks for pytest stubs and SQL validation queries."""
        assert "pytest" in review_script_source
        assert "validation" in review_script_source.lower()

    def test_configurable_model(self, review_script_source):
        """Model is configurable via REVIEW_MODEL env var."""
        assert "REVIEW_MODEL" in review_script_source

    def test_configurable_base_url(self, review_script_source):
        """API base URL is configurable via API_BASE_URL env var."""
        assert "API_BASE_URL" in review_script_source


# ===========================================================================
# generate_pr_description.py
# ===========================================================================

class TestPrDescriptionScript:
    """Validate the PR description generation script."""

    def test_script_parses_as_valid_python(self, pr_desc_script_source):
        """Script is syntactically valid Python."""
        ast.parse(pr_desc_script_source)

    def test_reads_diff_and_commits(self, pr_desc_script_source):
        """Reads both the diff and commit messages."""
        assert "/tmp/diff.txt" in pr_desc_script_source
        assert "/tmp/commits.txt" in pr_desc_script_source

    def test_reads_pr_template(self, pr_desc_script_source):
        """Loads the PR template from .github/pull_request_template.md."""
        assert "pull_request_template.md" in pr_desc_script_source

    def test_uses_pr_metadata(self, pr_desc_script_source):
        """Uses PR title and branch name from env vars."""
        assert "PR_TITLE" in pr_desc_script_source
        assert "PR_BRANCH" in pr_desc_script_source

    def test_handles_missing_diff(self, pr_desc_script_source):
        """Exits with error when diff is not available."""
        assert "sys.exit(1)" in pr_desc_script_source

    def test_instructs_jira_ticket_extraction(self, pr_desc_script_source):
        """Prompt tells the model to extract Jira ticket from branch/commits."""
        assert "Jira ticket" in pr_desc_script_source
        assert "branch name" in pr_desc_script_source
