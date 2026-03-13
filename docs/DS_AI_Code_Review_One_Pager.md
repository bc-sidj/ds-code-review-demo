# Data Solutions: AI-Powered Code Review & Testing Automation

## Current State

The 8-member DS team maintains the data pipelines powering BigCommerce's reporting and BI. Today, the team relies on a **fully manual process** for code reviews, test documentation, and rollout documentation. Each change is manually tested and recorded in a shared Word document, with the overall process taking **a day to a week** depending on complexity.

**The PR process today:**
1. Thoroughly test the change and build out the testing document
2. Create rollout document
3. Request a review once documentation and PR are ready
4. Address revisions, get approval, request a rollout

**Key pain points:** Inconsistent reviews across 30+ team standards, repetitive checklist-style feedback, manual Snowflake query writing and execution for data validation, and a bottleneck on senior engineers who are the only ones able to effectively review.

---

## What We're Building

By leveraging AI tools such as **Claude** and **Copilot**, we aim to automate code reviews and test case generation, **reducing turnaround time from days to hours**, while keeping humans in the loop for final validation and PR approvals.

**Claude in the IDE (PyCharm):** Engineers use Claude for real-time AI-assisted coding and pre-PR code review, loaded with DS team standards for context-aware feedback.

**Automated on Pull Requests (GitHub Actions + AI):** When a PR is opened, three layers of automated validation run and post results directly as PR comments:

| Layer | What It Does |
|-------|-------------|
| **Static Validators** | 20+ deterministic checks per file — missing documentation, unsafe SQL, credential exposure, compliance gaps |
| **AI-Generated Tests** | Reads the PR diff, generates and executes test cases specific to the changes |
| **Snowflake Validation** | Generates and executes data validation queries (row counts, hash comparisons, duplicate checks) — replaces the most time-consuming manual step |

---

## Estimated Impact

| | Today | With Automation |
|--|-------|-----------------|
| **PR cycle time** | 1 day – 1 week | Hours |
| **Testing + validation** | Manual queries + Word doc | Automated and reported on PR |
| **Standards coverage** | Varies by reviewer | 100% — every check, every time |
| **Review iterations** | Multiple rounds common | Fewer cycles — issues caught earlier |

### Hours Saved

| | |
|--|--|
| Team size | 8 engineers |
| Avg PRs/week | ~30 |
| Est. time saved per PR | ~1–1.5 hrs |
| **Hours saved per week** | **~30–45 hrs** |
| **Hours saved per month** | **~120–180 hrs** |

**Additional benefits:** Faster onboarding (instant standards feedback), consistent quality on critical issues, SOX compliance enforcement, and engineers refocused on higher-value work.
