USE DATABASE FUJI;

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
