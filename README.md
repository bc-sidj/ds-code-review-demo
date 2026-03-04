# DS Code Review Demo

A proof-of-concept for automated code review using Claude AI, tailored to the BigCommerce Data Solutions team's workflow and standards.

**No Anthropic API key required** — works with your existing Claude Pro/Max subscription.

## What's in this repo

```
ds-code-review-demo/
├── .claude/commands/
│   └── code-review.md              # /code-review slash command for PyCharm
├── .github/workflows/
│   └── claude-code-review.yml      # GitHub Action for automatic PR review
├── CLAUDE.md                        # Project memory — Claude reads this automatically
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
| **Layer 1** — Local review | PyCharm (Claude Code plugin) | You type `/code-review` | None (uses your Claude subscription) |
| **Layer 2** — PR review | GitHub Actions | Automatic on PR open/update | OAuth token from `claude setup-token` |

## Quick Start

### Prerequisites
- PyCharm with the **Claude Code plugin** installed
- A **Claude Pro or Max** subscription
- Git + GitHub CLI (`gh`)

### Step 1: Clone and initialize
```bash
git clone https://github.com/<your-username>/ds-code-review-demo.git
cd ds-code-review-demo
```

### Step 2: Set up GitHub Action auth (OAuth token — no API key needed)

1. Open your terminal and run:
   ```bash
   claude setup-token
   ```
2. Copy the OAuth token it generates
3. Go to your repo on GitHub → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
   - Name: `CLAUDE_CODE_OAUTH_TOKEN`
   - Value: paste the token from step 2
4. Install the Claude GitHub App: https://github.com/apps/claude
   - Grant it access to your `ds-code-review-demo` repo

### Step 3: Test the local review (Layer 1)
```bash
# Create a feature branch with the buggy files
git checkout -b feature/DS-9999-test-buggy-code
cp dags/buggy/dag_store_metrics_buggy.py dags/
cp ddl/fuji/vz_apps/buggy/vw_store_summary_buggy.sql ddl/fuji/vz_apps/
git add .
git commit -m "DS-9999 Add store metrics DAG and view"
```

Now open the project in PyCharm, open the Claude Code panel, and type:
```
/code-review
```

Claude will analyze the diff and generate a report at `code_reviews/code_review_feature-DS-9999-test-buggy-code.md`.

### Step 4: Test the GitHub Action (Layer 2)
```bash
git push -u origin feature/DS-9999-test-buggy-code
```

Go to GitHub and open a Pull Request against `main`. Within ~90 seconds, Claude will post a review comment on the PR.

You can also tag `@claude` in any PR comment to ask follow-up questions.

### Step 5: Try the clean version
```bash
git checkout main
git checkout -b feature/DS-4521-clean-metrics
cp dags/clean/dag_store_metrics_clean.py dags/
cp ddl/fuji/vz_apps/clean/vw_store_summary_clean.sql ddl/fuji/vz_apps/
git add .
git commit -m "DS-4521 Add store metrics DAG and view"
```

Run `/code-review` again — this time the report should show mostly green checks.

## What gets reviewed

| Category | Checks |
|----------|--------|
| **DS Workflow** | Jira ticket in commits, testing doc reminder, rollout doc reminder |
| **Python/Airflow** | DAG structure, task deps, retries, idempotency, no hardcoded secrets, edge cases |
| **SQL/DDL/DML** | Fully qualified names, object comments, WHERE on DML, sp_rollout tags, edge cases |
| **Auto-generated** | pytest stubs for Python, validation queries for SQL |

## Moving to production (dw_airflow)

When the team is ready, copy these into the production repo:
```bash
cp -r .claude/ /path/to/dw_airflow/.claude/
cp -r .github/ /path/to/dw_airflow/.github/
cp CLAUDE.md /path/to/dw_airflow/CLAUDE.md
```

Then:
1. Add `code_reviews/*.md` to the dw_airflow `.gitignore`
2. Run `claude setup-token` and add the token as `CLAUDE_CODE_OAUTH_TOKEN` in dw_airflow repo secrets
3. Install the Claude GitHub App on the dw_airflow repo

## Customizing the review

- **Local review rules:** Edit `.claude/commands/code-review.md`
- **PR review rules:** Edit the `prompt:` section in `.github/workflows/claude-code-review.yml`
- **Project context:** Edit `CLAUDE.md` to add team conventions Claude should know about

All plain text files — any team member can update them.
