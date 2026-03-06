# DS Code Review Demo

A proof-of-concept for automated code review using AI, tailored to the BigCommerce Data Solutions team's workflow and standards.

## What's in this repo

```
ds-code-review-demo/
├── .claude/commands/
│   └── code-review.md              # Review instructions (reference prompt for Cursor)
├── .github/
│   ├── scripts/
│   │   └── review.py               # Python script that calls AI API for review
│   └── workflows/
│       └── claude-code-review.yml  # GitHub Action for automatic PR review
├── CLAUDE.md                        # Project memory — AI reads this automatically
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
│   └── team_proposal.md            # Proposal document to share with the team
└── README.md                        # You are here
```

## Two Layers of Automation

| Layer | Where | Trigger | Auth needed |
|-------|-------|---------|-------------|
| **Layer 1** — Local review | Cursor IDE | Ask the AI to follow the review instructions | None (uses your Cursor subscription) |
| **Layer 2** — PR review | GitHub Actions | Automatic on PR open/update | OpenRouter API key (or any OpenAI-compatible key) |

## Quick Start

### Prerequisites
- **Cursor** IDE installed
- Git + GitHub CLI (`gh`)
- An API key from one of these providers (for the GitHub Action):

| Provider | Cost | How to get a key |
|----------|------|------------------|
| **OpenRouter** (recommended) | Free credits on signup, then pay-per-use | [openrouter.ai/keys](https://openrouter.ai/keys) |
| **OpenAI** | Pay-per-use | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) |
| **Anthropic** | Pay-per-use | [console.anthropic.com](https://console.anthropic.com/) |

### Step 1: Clone and initialize
```bash
git clone https://github.com/<your-username>/ds-code-review-demo.git
cd ds-code-review-demo
```

### Step 2: Set up GitHub Action auth

**Option A — OpenRouter (recommended, no Anthropic key needed)**
1. Sign up at [openrouter.ai](https://openrouter.ai) (free credits included)
2. Go to [openrouter.ai/keys](https://openrouter.ai/keys) and create an API key
3. In your GitHub repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
   - Name: `OPENROUTER_API_KEY`
   - Value: paste the key

**Option B — OpenAI or Anthropic directly**
1. Add your key as a GitHub secret named `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`
2. In `.github/workflows/claude-code-review.yml`, uncomment the `API_BASE_URL` and `REVIEW_MODEL` lines and set them to match your provider:
   - OpenAI: `API_BASE_URL: "https://api.openai.com/v1"` / `REVIEW_MODEL: "gpt-4o"`
   - Anthropic: `API_BASE_URL: "https://api.anthropic.com/v1"` / `REVIEW_MODEL: "claude-sonnet-4-20250514"`

No GitHub App installation or OAuth token setup required.

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

> Follow the instructions in `.claude/commands/code-review.md` to review the changes on this branch.

Cursor will analyze the diff and generate a report at `code_reviews/code_review_<branch-name>.md`.

### Step 4: Test the GitHub Action (Layer 2)
```bash
git push -u origin feature/DS-9999-test-buggy-code
```

Go to GitHub and open a Pull Request against `main`. The GitHub Action will call the Anthropic API and post a structured review comment on the PR within ~90 seconds.

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
cp -r .claude/ /path/to/dw_airflow/.claude/
cp -r .github/ /path/to/dw_airflow/.github/
cp CLAUDE.md /path/to/dw_airflow/CLAUDE.md
```

Then:
1. Add `code_reviews/*.md` to the dw_airflow `.gitignore`
2. Add your `OPENROUTER_API_KEY` (or other provider key) as a repository secret in the dw_airflow repo settings
3. Customize `CLAUDE.md`, `code-review.md`, and the workflow prompt as standards evolve

## Customizing the review

- **Local review rules:** Edit `.claude/commands/code-review.md`
- **PR review prompt:** Edit `.github/scripts/review.py` (the `REVIEW_PROMPT` variable)
- **Project context:** Edit `CLAUDE.md` to add team conventions the AI should know about
- **Workflow triggers:** Edit `.github/workflows/claude-code-review.yml`

All plain text files — any team member can update them.
