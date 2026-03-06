# DS Code Review Demo

A proof-of-concept for automated code review using AI, tailored to the BigCommerce Data Solutions team's workflow and standards.

## What's in this repo

```
ds-code-review-demo/
├── .github/
│   ├── scripts/
│   │   └── review.py               # Python script that calls AI API for review
│   └── workflows/
│       └── code-review.yml         # GitHub Action for automatic PR review
├── CONTEXT.md                       # Project memory — AI reads this automatically
├── dags/
│   ├── buggy/
│   │   └── dag_store_metrics_buggy.py   # 17 intentional issues for demo
│   └── clean/
│       └── dag_store_metrics_clean.py   # Fixed version (passes all checks)
├── ddl/fuji/vz_apps/
│   ├── buggy/
│   │   └── vw_store_summary_buggy.sql   # 13 intentional issues for demo
│   └── clean/
│       └── vw_store_summary_clean.sql   # Fixed version (passes all checks)
├── code_reviews/                    # Local review reports land here (gitignored)
├── docs/
│   ├── code-review-instructions.md  # Review checklist (reference prompt for Cursor)
│   ├── cursor_setup_prompt.md       # Copy-paste prompt for first-time setup
│   └── team_proposal.md            # Proposal document to share with the team
└── README.md                        # You are here
```

## Two Layers of Automation

| Layer | Where | Trigger | Auth needed |
|-------|-------|---------|-------------|
| **Layer 1** — Local review | Cursor IDE | Ask the AI to follow the review instructions | None (uses your Cursor subscription) |
| **Layer 2** — PR review | GitHub Actions | Automatic on PR open/update | OpenAI API key stored as GitHub secret |

## Quick Start

### Prerequisites
- **Cursor** IDE installed
- Git + GitHub CLI (`gh`)
- An **OpenAI API key** (for the GitHub Action) — get one at [platform.openai.com/api-keys](https://platform.openai.com/api-keys)

### Step 1: Clone and initialize
```bash
git clone https://github.com/<your-username>/ds-code-review-demo.git
cd ds-code-review-demo
```

### Step 2: Set up GitHub Action auth (OpenAI API key)

1. Go to your repo on GitHub → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
   - Name: `OPENAI_API_KEY`
   - Value: paste your OpenAI API key

That's it — no GitHub App installation or OAuth token setup required.

> **Want to use a different provider?** The review script supports any OpenAI-compatible API. Set `API_BASE_URL` and `REVIEW_MODEL` in the workflow env vars to point at OpenRouter, Anthropic, or another provider.

### Step 3: Test the local review (Layer 1)
```bash
# Create a feature branch with the buggy files
git checkout -b feature/DS-9999-test-buggy-code
cp dags/buggy/dag_store_metrics_buggy.py dags/
cp ddl/fuji/vz_apps/buggy/vw_store_summary_buggy.sql ddl/fuji/vz_apps/
git add .
git commit -m "DS-9999 Add store metrics DAG and view"
```

Now open the project in **Cursor** and tell the AI:

> Follow the instructions in `docs/code-review-instructions.md` to review the changes on this branch.

Cursor will analyze the diff and generate a report at `code_reviews/code_review_<branch-name>.md`.

### Step 4: Test the GitHub Action (Layer 2)
```bash
git push -u origin feature/DS-9999-test-buggy-code
```

Go to GitHub and open a Pull Request against `main`. The GitHub Action will call GPT-4o and post a structured review comment on the PR within ~90 seconds.

You can also tag `@review` in any PR comment to trigger a follow-up review.

### Step 5: Try the clean version
```bash
git checkout main
git checkout -b feature/DS-4521-clean-metrics
cp dags/clean/dag_store_metrics_clean.py dags/
cp ddl/fuji/vz_apps/clean/vw_store_summary_clean.sql ddl/fuji/vz_apps/
git add .
git commit -m "DS-4521 Add store metrics DAG and view"
```

Ask Cursor to run the code review again — this time the report should show mostly green checks.

## What gets reviewed

| Category | Checks |
|----------|--------|
| **DS Workflow** | Jira ticket in commits, testing doc reminder, rollout doc reminder |
| **Python/Airflow** | DAG structure, task deps, retries, idempotency, no hardcoded secrets, edge cases |
| **SQL/DDL/DML** | Fully qualified names, object comments, WHERE on DML, sp_rollout tags, edge cases |
| **Auto-generated** | pytest stubs for Python, validation queries for SQL |

## Severity Levels

| Level | Meaning |
|-------|---------|
| ❌ CRITICAL | Must fix before merge (e.g., missing WHERE clause on DELETE, hardcoded credentials) |
| ⚠️ WARNING | Should address (e.g., missing on_failure_callback, unhandled NULLs) |
| ℹ️ SUGGESTION | Nice to have (readability, performance) |
| ✅ PASS | Explicitly acknowledged good practices |

## Moving to production (dw_airflow)

When the team is ready, copy these into the production repo:
```bash
cp -r .github/ /path/to/dw_airflow/.github/
cp CONTEXT.md /path/to/dw_airflow/CONTEXT.md
cp docs/code-review-instructions.md /path/to/dw_airflow/docs/code-review-instructions.md
```

Then:
1. Add `code_reviews/*.md` to the dw_airflow `.gitignore`
2. Add your `OPENAI_API_KEY` as a repository secret in the dw_airflow repo settings
3. Customize `CONTEXT.md`, `code-review-instructions.md`, and the workflow prompt as standards evolve

## Customizing the review

- **Local review rules:** Edit `docs/code-review-instructions.md`
- **PR review prompt:** Edit `.github/scripts/review.py` (the `REVIEW_PROMPT` variable)
- **Project context:** Edit `CONTEXT.md` to add team conventions the AI should know about
- **Workflow triggers:** Edit `.github/workflows/code-review.yml`

All plain text files — any team member can update them.
