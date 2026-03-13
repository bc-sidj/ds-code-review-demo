# Data Solutions: AI-Powered Code Review & Testing Automation

## Current State

The 8-member Data Solutions (DS) team maintains the data pipelines and analytics infrastructure that powers BigCommerce's reporting and business intelligence. Today, the team relies on a **fully manual process** for code reviews, test documentation, and rollout documentation.

### The PR Process Today

Once an engineer's branch is ready for testing, the process follows four steps:

1. **Test the change** — Thoroughly test the code change and build out a testing document (recorded in a shared Word document)
2. **Create rollout document** — Write deployment and rollback instructions
3. **Request a review** — Submit the PR with documentation for peer review
4. **Address revisions and roll out** — Incorporate feedback, get approval, then request a production rollout (SOX compliance requires a separate deployer)

**End-to-end, the PR process takes anywhere from a day to a week**, depending on complexity and assuming blockers are resolved relatively quickly. Engineers typically work on multiple tickets in parallel — while waiting on a review or blocker for one, they make progress on another.

### Where the Friction Is

Each step involves manual, repetitive work across multiple tools (IDE, Snowflake, Jira, Google Drive, GitHub):

- **Testing documentation is manual** — Engineers write and execute Snowflake validation queries by hand, then record results in Word documents
- **Reviews are inconsistent** — Different reviewers catch different issues; no single reviewer consistently applies all 30+ team standards
- **Repetitive feedback** — The majority of review comments cover the same recurring issues (missing documentation tags, unqualified table names, missing safety clauses)
- **Senior engineer bottleneck** — Only experienced team members can effectively review, creating a dependency that limits throughput

---

## What We're Building

By leveraging AI tools such as **Claude** and **Copilot**, we aim to automate code reviews and test case generation, **significantly reducing turnaround time to a few hours**, while keeping humans in the loop for final validation and PR approvals.

### How DS Uses Claude Today

**In the IDE (Claude via PyCharm):**
- Engineers use Claude integrated into their development environment for real-time AI-assisted coding
- Claude is loaded with DS team standards and conventions, providing context-aware suggestions as engineers write DAGs and SQL
- Engineers run a local AI-powered code review before pushing code — catching issues before they reach the team

**Automated on Pull Requests (GitHub Actions + AI):**
- When a PR is opened, GitHub Actions automatically triggers AI-powered review and testing
- A structured code review is posted directly as a PR comment with a READY / NOT READY verdict
- Test cases are generated, executed, and results reported — no manual effort required

### The Three-Layer Automation

| Layer | What It Does | Speed |
|-------|-------------|-------|
| **Static Validators** | 20+ deterministic checks per file — catches missing documentation, unsafe SQL patterns, credential exposure, and compliance gaps. Backed by 200+ unit tests. | Seconds |
| **AI-Generated Tests** | Reads the PR diff and team standards, generates test cases specific to the changes. Tests are executed automatically with PASS/FAIL results. | ~15 sec |
| **Snowflake Validation** | AI generates data validation queries (row counts, hash comparisons, duplicate checks), executes them against Snowflake, and reports results. Replaces the most time-consuming manual step. | ~10 sec |

All three layers run automatically on every PR and post a detailed report organized by five testing categories: Data Integrity, Schema Compliance, Regression, Edge Cases, and Downstream Impact.

### How the Process Changes

| Step | Today (Manual) | With Automation |
|------|---------------|-----------------|
| **1. Testing** | Manually write + run Snowflake queries, record in Word doc | AI generates and executes validation queries automatically; report posted on PR |
| **2. Rollout doc** | Written manually | No change (remains manual) |
| **3. Code review** | Reviewer manually checks 30+ standards across Python + SQL | AI runs all checks in ~30 seconds, reviewer focuses on logic and architecture |
| **4. Revisions** | Back-and-forth over days | Most standards issues caught before review — fewer revision cycles |

---

## Estimated Impact

It's difficult to give precise time estimates because complexity varies significantly across PRs. However, the automation targets the most repetitive and time-consuming portions of the process:

| Metric | Current (Manual) | With Automation |
|--------|-----------------|-----------------|
| End-to-end PR cycle | 1 day – 1 week | **Hours instead of days** |
| Testing + validation effort | Manual query writing, execution, Word doc | **Automated** — generated, executed, and reported |
| Standards coverage per review | Varies by reviewer | **100%** — every check, every time |
| Review iterations | Multiple rounds common | **Fewer cycles** — issues caught before review |

### Weekly Team Savings

| | |
|---|---|
| Team size | 8 engineers |
| Avg PRs per week | ~30 |
| Estimated time saved per PR | ~1–1.5 hrs (testing + review effort) |
| **Estimated hours saved per week** | **~30–45 hrs/week** |
| **Estimated hours saved per month** | **~120–180 hrs/month** |

These hours are redirected from repetitive checklist work to higher-value activities: building new pipelines, improving data quality, and delivering insights faster.

### Additional Benefits

- **Faster onboarding** — New engineers receive instant, standards-aware feedback without waiting for a senior reviewer
- **Consistent quality** — Critical issues (credential exposure, unsafe DELETE/UPDATE, data integrity) are caught 100% of the time
- **SOX compliance** — Automated enforcement of separation of duties and audit trail documentation
- **Human in the loop** — AI handles the repetitive validation; engineers focus on logic, architecture, and final approval

---

## What's Next

- **Production rollout** to the team's primary repository
- **Expanded Snowflake validators** for team-specific patterns (MRR reconciliation, financial metric validation)
- **Slack integration** for real-time alerts on critical findings
- **Metrics tracking** to measure review coverage, common issues, and improvement trends over time
