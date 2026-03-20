-- =============================================================
-- INTENTIONAL ISSUES FOR DEMO
-- This SQL file has 13 issues that the automated review should catch.
-- See ddl/fuji/vz_apps/clean/vw_store_summary_clean.sql for the fixed version.
-- =============================================================

-- BUG 1: Missing USE DATABASE FUJI
-- BUG 2: Has USE SCHEMA (should not be in DDL files per DS standards)
USE SCHEMA vz_apps;

-- BUG 3: Missing COMMENT with Jira ticket number
CREATE OR REPLACE VIEW store_summary AS  -- BUG 4: Not fully qualified (should be vz_apps.vw_store_summary)

SELECT
    s.store_id,
    s.store_name,
    s.created_date,
    -- BUG 5: Using SELECT * from a subquery
    sub.*,
    -- BUG 6: Divide by zero risk — no NULLIF or CASE protection
    s.total_revenue / s.total_orders AS avg_order_value,
    -- BUG 7: NULL propagation — LEFT JOIN but no COALESCE on nullable field
    p.plan_name,
    p.plan_tier
FROM
    STORE s  -- BUG 8: Not fully qualified (should be FIL.STORE)
LEFT JOIN
    plan p ON s.plan_id = p.plan_id  -- BUG 9: Not fully qualified
INNER JOIN (
    SELECT
        store_id,
        COUNT(*) AS order_count,
        SUM(amount) AS total_amount
    FROM
        orders  -- BUG 10: Not fully qualified
    -- BUG 11: No WHERE clause — scanning entire orders table
    GROUP BY store_id
) sub ON s.store_id = sub.store_id;
-- BUG 12: INNER JOIN means stores with zero orders are silently dropped — should this be LEFT?

-- BUG 13: DML without WHERE clause — CRITICAL
UPDATE FIL.STORE_METRICS
SET last_refreshed = CURRENT_TIMESTAMP();
-- This updates EVERY row — is that intentional?
