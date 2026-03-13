## What/Why?

<!-- Jira ticket(s) and brief description of the change -->
- [DS-XXXX](https://bigcommercecloud.atlassian.net/browse/DS-XXXX)

<!-- Brief description of what this PR does and why -->

## Acceptance Criteria

<!-- What needs to be true for this PR to be considered complete? -->
- 

## Type of Change

- [ ] New DAG / pipeline
- [ ] DAG modification
- [ ] DDL/DML change (Snowflake)
- [ ] Bug fix
- [ ] Refactor / cleanup
- [ ] Documentation only

## Changes Made

<!-- List the specific changes in this PR -->
- 

## Testing

> Testing evidence is **auto-generated** by the CI workflow and posted as a PR comment.
> It includes: static validation (20+ checks), AI-generated tests, and Snowflake query execution.
> The report artifact is also uploaded to the Actions tab for audit purposes.

- [ ] Automated test report posted (check PR comments)
- [ ] Edge cases verified (empty data, NULLs, duplicates, date boundaries)
- [ ] Additional manual testing notes (if any): <!-- add notes here -->

## Rollout/Rollback

> The rollout playbook is **auto-generated** by the CI workflow and posted as part of the test report PR comment.
> Review the "Rollout Playbook" section and adjust before rollout.

- [ ] Rollout playbook reviewed in test report comment
- [ ] Manual additions (if any): <!-- add notes here -->

## Checklist

- [ ] Jira ticket number is in at least one commit message
- [ ] No hardcoded credentials or environment-specific paths
- [ ] All SQL object references are fully qualified (e.g., `FIL.STORE`)
- [ ] All new/modified objects have a `COMMENT` with the Jira ticket
- [ ] All `UPDATE`/`DELETE` statements have a `WHERE` clause
- [ ] DAGs use `catchup=False` (unless backfill is intended)
- [ ] `default_args` includes `owner`, `start_date`, `retries`, `retry_delay`
- [ ] `on_failure_callback` is set for critical tasks
- [ ] No unused imports
- [ ] Code is idempotent (safe to re-run)

## SOX Compliance Reminder

> The PR submitter cannot be the person who rolls out to production. Ensure a separate team member handles the production rollout.
