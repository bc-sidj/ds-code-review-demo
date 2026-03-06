## Summary

<!-- Brief description of what this PR does and why -->

## Jira Ticket

<!-- Link to the Jira ticket (required) -->
- [ ] Ticket: [DS-XXXX](https://bigcommerce.atlassian.net/browse/DS-XXXX)

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

<!-- Describe how this was tested -->
- [ ] Airflow tasks tested with production-equivalent data
- [ ] SQL changes validated in Snowflake (dev/staging)
- [ ] Edge cases verified (empty data, NULLs, duplicates, date boundaries)
- [ ] Testing documentation saved to [Google Drive](https://drive.google.com/) at `Ops & Analytics/Data Solutions/Unit Testing + Rollout/`

## Rollout

<!-- Required for DDL/DML or complex Airflow changes -->
- [ ] Rollout doc created and saved to Google Drive
- [ ] Rollout doc includes `sp_rollout('start', ...)` and `sp_rollout('end', ...)`
- [ ] Backout plan documented
- [ ] N/A — no rollout needed for this change

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
