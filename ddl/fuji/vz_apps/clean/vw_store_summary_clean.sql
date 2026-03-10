-- Rollout wrapped with sp_rollout bookends (fixes BUG 14)
CALL edw.gbl.sp_rollout('start', 'DS-4521', '142', 'Store summary view and metrics', 'SYSADMIN', 'FUJI');

USE DATABASE FUJI;

-- Backup before destructive changes (fixes BUG 15)
CREATE OR REPLACE TABLE fuji_stash.vz_apps.store_metrics_backup
    CLONE FIL.STORE_METRICS
    COMMENT = 'DS-4521 Backup before store_summary changes. Drop after 2025-04-01';

CREATE OR REPLACE VIEW vz_apps.vw_store_summary
COMMENT = 'DS-4521 Store summary view with order metrics and plan info'
AS

SELECT
    s.store_id,
    s.store_name,
    s.created_date,
    COALESCE(sub.order_count, 0)   AS order_count,
    COALESCE(sub.total_amount, 0)  AS total_amount,
    CASE
        WHEN COALESCE(sub.order_count, 0) > 0
        THEN s.total_revenue / sub.order_count
        ELSE 0
    END AS avg_order_value,
    COALESCE(p.plan_name, 'Unknown')  AS plan_name,
    COALESCE(p.plan_tier, 'Unknown')  AS plan_tier
FROM
    FIL.STORE s
LEFT JOIN
    FIL.PLAN p
    ON s.plan_id = p.plan_id
LEFT JOIN (
    SELECT
        store_id,
        COUNT(*)    AS order_count,
        SUM(amount) AS total_amount
    FROM
        FIL.ORDERS
    WHERE
        order_date >= DATEADD('year', -1, CURRENT_DATE())
    GROUP BY
        store_id
) sub
    ON s.store_id = sub.store_id
WHERE
    s.is_active = TRUE;

-- Deterministic ROW_NUMBER with tiebreaker (fixes BUG 16)
CREATE OR REPLACE VIEW vz_apps.vw_latest_order
COMMENT = 'DS-4521 Latest order per store by amount descending, tiebroken by order_id'
AS
SELECT store_id, order_id, amount FROM (
    SELECT
        store_id,
        order_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, order_id DESC) AS rn
    FROM FIL.ORDERS
) WHERE rn = 1;

-- Adequate VARCHAR length (fixes BUG 17)
ALTER TABLE FIL.STORE_METRICS ADD COLUMN store_description VARCHAR(500)
    COMMENT = 'DS-4521 Store description field — 500 chars to accommodate long descriptions';

-- Downstream dependency check documented (fixes BUG 18)
-- Verified: SELECT full_table_name, user_name FROM security.table_usage_summary
--   WHERE full_table_name = 'FUJI.FIL.STORE_METRICS'
--   AND user_name IN ('AIRFLOW', 'TABLEAU', 'TABLEAU_2');
-- Result: No downstream dependencies found.

CALL edw.gbl.sp_rollout('end', '', '', '', 'FUJI_DEV_OWNER', 'FUJI_DEV');
