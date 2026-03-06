# Automated Code Review with AI — Team Proposal

**Author:** Siddharth Jasani
**Date:** March 2026
**Status:** Proof of Concept — Ready for Team Review

---

## The Problem

Our current code review process is entirely manual. Every PR waits for a team member to find time to review it, and reviewers spend significant time catching routine issues — missing Jira ticket numbers, unqualified SQL object names, hardcoded credentials, missing error handling — before they can even get to the meaningful logic review.

This slows down our velocity, creates bottlenecks when reviewers are busy, and means some issues slip through to production.

---

## The Proposal

Add an **automated first-pass review** powered by AI that runs at two points in our workflow:

**Layer 1 — Before the PR (in Cursor)**
Ask Cursor's AI to follow the review instructions in `docs/code-review-instructions.md`. It reviews your diff locally and generates a report in under a minute. You fix issues before anyone else ever sees your code.

**Layer 2 — On the PR (GitHub Action)**
An automated GitHub Action that triggers every time a PR is opened or updated. The workflow calls the OpenAI API with the PR diff and posts a structured review comment directly on the PR — before a human reviewer opens it.

The human reviewer still has final say. AI just handles the first pass so the human can focus on logic, architecture, and business correctness.

---

## What the Review Checks (Tailored to DS Standards)

**DS Workflow Compliance**
- Jira ticket number present in commit messages
- Reminders for testing and rollout documentation
- DDL-only simplified process detection

**Python / Airflow**
- DAG structure (catchup, default_args, schedule)
- Task dependencies (no orphaned tasks, no circular chains)
- Error handling (retries, on_failure_callback)
- Security (no hardcoded credentials — must use Airflow Connections/Variables)
- Idempotency (safe to re-run)
- Edge cases (empty data, NULLs, date boundaries)

**SQL / DDL / DML**
- Fully qualified object names (FIL.STORE, not just STORE)
- Object COMMENTs with ticket numbers
- No USE SCHEMA statements
- WHERE clauses on all UPDATE/DELETE
- sp_rollout query tags in rollout docs
- Edge cases (NULL propagation, duplicates, divide-by-zero)

**Auto-Generated Test Cases**
- Python: pytest stubs covering happy path, empty input, NULLs, date boundaries
- SQL: validation queries for row counts, NULLs, duplicates, range checks

---

## What Changes in Our Workflow

```
CURRENT PROCESS                    NEW PROCESS
─────────────────                  ──────────────────────────
Write code                         Write code
  │                                  │
  │                                Ask Cursor AI to review       ← NEW
  │                                  │
  │                                Fix issues found locally      ← NEW
  │                                  │
Complete testing                   Complete testing
  │                                  │
Create PR                          Create PR
  │                                  │
  │                                AI reviews PR (auto)          ← NEW
  │                                  │
Wait for human review              Human reviews (faster — routine
  │                                  issues already caught)
  │                                  │
Approved → Rollout                 Approved → Rollout
```

The key benefit: **human reviewers spend their time on what matters** — business logic, architecture decisions, and edge cases that require domain knowledge — instead of catching formatting and standards violations.

---

## What This Does NOT Replace

- Human approval is still required on every PR
- Separation of duties for production rollout (SOX compliance) is unchanged
- Testing documentation and rollout docs are still created manually
- The AI does not have access to Snowflake or Airflow — it reviews code only

---

## Proof of Concept

I've built a demo repo (`ds-code-review-demo`) that you can clone and try yourself. It includes:

1. **A buggy Airflow DAG** — 17 intentional issues including hardcoded passwords, missing error handling, deprecated imports, and orphaned tasks
2. **A buggy SQL file** — 13 intentional issues including unqualified names, missing comments, divide-by-zero risks, and DML without WHERE clauses
3. **Clean versions of both** — showing what "passing" code looks like
4. **The full automation setup** — review instructions + GitHub Action, ready to test

### How to try it yourself
```bash
# 1. Clone the demo repo
git clone https://github.com/<your-username>/ds-code-review-demo.git
cd ds-code-review-demo

# 2. Create a feature branch with the buggy files
git checkout -b feature/DS-9999-test-buggy-code
cp dags/buggy/* dags/
cp ddl/fuji/vz_apps/buggy/* ddl/fuji/vz_apps/
git add . && git commit -m "DS-9999 Add store metrics DAG and view"

# 3. Open in Cursor and ask the AI to review
#    "Follow the instructions in docs/code-review-instructions.md to review changes on this branch."

# 4. Push and open a PR to see the GitHub Action in action
git push -u origin feature/DS-9999-test-buggy-code
# Then open a PR on GitHub
```

---

## Cost and Maintenance

- **API cost:** Each review uses roughly 2,000-4,000 tokens (~$0.01-0.04 per review with GPT-4o). At 20 PRs/week, that's under $5/month.
- **Maintenance:** The review checklist lives in markdown/YAML/Python files in the repo. Any team member can update the review rules by editing those files — no special tooling required.
- **No new accounts/tools needed:** Uses our existing GitHub Actions infrastructure + an API key (OpenRouter, OpenAI, or Anthropic) stored as a GitHub secret.

---

## Next Steps

1. **Try the demo** — clone the repo and run the review on the buggy examples
2. **Team discussion** — is the review checklist complete? Any DS-specific patterns to add?
3. **Pilot on real work** — pick 2-3 real PRs and run the automated review alongside the manual one. Compare results.
4. **Roll out to dw_airflow** — once we're confident, copy `.github/`, `CONTEXT.md`, and `docs/code-review-instructions.md` into the production repo.

---

## FAQ

**Q: Will this slow down PRs?**
No — the GitHub Action runs in parallel and typically completes in under 90 seconds. The review comment appears before a human would normally get to it.

**Q: Can the AI approve PRs?**
No. It only comments. A human must still approve and merge. SOX separation of duties is fully maintained.

**Q: What if it gives a false positive?**
Treat it like any review comment — if it's wrong, ignore it. Over time we can tune the checklist to reduce noise.

**Q: Does it see our production data?**
No. The AI only sees the code diff in the PR. It has no access to Snowflake, Airflow, or any production systems.

**Q: Can we customize what it checks?**
Yes — the entire review checklist is defined in `docs/code-review-instructions.md` (for local reviews) and in `.github/scripts/review.py` (for PR reviews). Edit those files to add, remove, or modify any check.
