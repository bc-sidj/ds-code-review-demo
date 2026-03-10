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

<!-- Link to testing documentation in Google Drive -->
- [ ] [Testing documentation](https://drive.google.com/)
- [ ] Edge cases verified (empty data, NULLs, duplicates, date boundaries)

## Rollout/Rollback

**Airflow changes:** <!-- e.g., Github Actions, manual deploy, none -->

**Database changes:** <!-- e.g., DDL/DML scripts, none -->

**AWS Variables:**
- Variable name: <!-- e.g., /airflow/variables/ds_variable_name -->
- Value: <!-- e.g., stages.production.example -->

**Environment Variables (Astronomer):** <!-- e.g., None -->

**Re-run all relevant jobs in production:** <!-- e.g., None, or list jobs -->

**Other changes:** <!-- e.g., None -->

**Rollback considerations:** <!-- e.g., None, or describe rollback steps -->

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
