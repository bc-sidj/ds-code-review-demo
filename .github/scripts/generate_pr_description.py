#!/usr/bin/env python3
"""Auto-generate a populated PR description from the diff and commit history.

Reads the PR diff and commit messages, then calls an AI model to fill in
the PR description template with actual content.

Environment variables:
  OPENAI_API_KEY      — API key (also checks OPENROUTER_API_KEY, ANTHROPIC_API_KEY)
  API_BASE_URL        — Base URL (defaults to https://api.openai.com/v1)
  REVIEW_MODEL        — Model name (defaults to gpt-4o)
  PR_TITLE            — Title of the pull request
  PR_BRANCH           — Branch name of the pull request
"""

import os
import sys
import pathlib

from openai import OpenAI

DIFF_PATH = pathlib.Path("/tmp/diff.txt")
COMMITS_PATH = pathlib.Path("/tmp/commits.txt")
CONTEXT_MD_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / "CONTEXT.md"
TEMPLATE_PATH = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", ".")) / ".github" / "pull_request_template.md"

MAX_DIFF_CHARS = 80_000
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o"

PROMPT = """\
You are a helpful assistant that writes pull request descriptions for the \
BigCommerce Data Solutions (DS) team.

{project_context}

Here is the PR description template the team uses:

```markdown
{template}
```

Below are the commit messages and the git diff for this PR.

**PR Title:** {pr_title}
**Branch:** {pr_branch}

**Commit messages:**
{commits}

**Diff:**
```diff
{diff_content}
```

Your task: Fill in the PR template with actual content based on the diff and \
commits above. Follow these rules:

1. **What/Why?** — Extract the Jira ticket number (DS-XXXX or ANALYTICS-XXXX) from the \
branch name or commit messages. Format it as a link to \
`https://bigcommercecloud.atlassian.net/browse/DS-XXXX`. Write a brief 1-2 sentence \
description of what this PR does and why.
2. **Acceptance Criteria** — Based on the diff, list what this PR accomplishes. \
Be specific (e.g., "Create DAG t_store_metrics to export store data daily").
3. **Type of Change** — Check the appropriate boxes based on the file types changed.
4. **Changes Made** — List the specific changes: files added/modified, new tasks, \
SQL objects created, etc.
5. **Testing** — Leave the testing doc link as a placeholder for the developer to fill in. \
Leave checkboxes unchecked as reminders.
6. **Rollout/Rollback** — Fill in based on what the code changes:
   - If there are Airflow DAG changes: "Airflow changes: Github Actions"
   - If there are DDL/DML changes: list the database changes
   - If Airflow Variables are referenced in the code, list them under AWS Variables
   - If no changes for a section, write "None"
   - Rollback considerations: describe what would need to be undone
7. **Checklist** — Check items that are clearly satisfied by the code. \
Leave items unchecked if they are violations or cannot be confirmed from the diff.
8. **SOX Compliance Reminder** — Keep as-is.

Output ONLY the filled-in markdown. No extra commentary before or after.
"""


def get_api_key() -> str:
    for var in ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var)
        if key:
            return key
    print(
        "No API key found. Set OPENAI_API_KEY as an environment variable.",
        file=sys.stderr,
    )
    sys.exit(1)


def load_file(path: pathlib.Path) -> str:
    if path.exists():
        return path.read_text().strip()
    return ""


def main() -> None:
    diff_content = load_file(DIFF_PATH)
    if not diff_content:
        print("No diff found. Cannot generate PR description.", file=sys.stderr)
        sys.exit(1)

    if len(diff_content) > MAX_DIFF_CHARS:
        diff_content = (
            diff_content[:MAX_DIFF_CHARS]
            + "\n\n... [diff truncated due to size] ..."
        )

    commits = load_file(COMMITS_PATH) or "(no commits found)"
    template = load_file(TEMPLATE_PATH) or "(no template found)"

    context_md = load_file(CONTEXT_MD_PATH)
    project_context = (
        f"Here are the project context and team standards:\n\n{context_md}"
        if context_md
        else ""
    )

    pr_title = os.environ.get("PR_TITLE", "(untitled)")
    pr_branch = os.environ.get("PR_BRANCH", "(unknown)")

    prompt = PROMPT.format(
        project_context=project_context,
        template=template,
        pr_title=pr_title,
        pr_branch=pr_branch,
        commits=commits,
        diff_content=diff_content,
    )

    api_key = get_api_key()
    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL)
    model = os.environ.get("REVIEW_MODEL", DEFAULT_MODEL)

    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    print(response.choices[0].message.content)


if __name__ == "__main__":
    main()
