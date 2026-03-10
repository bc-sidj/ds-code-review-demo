USE SCHEMA vz_apps;

CREATE OR REPLACE VIEW order_fulfillment_summary AS

SELECT
    o.order_id,
    o.store_id,
    s.store_name,
    o.order_date,
    o.total_amount,
    o.shipping_cost,
    o.total_amount / o.shipping_cost AS cost_ratio,
    p.plan_name,
    p.plan_tier,
    sub.*
FROM
    ORDERS o
JOIN
    STORE s ON o.store_id = s.store_id
LEFT JOIN
    plan p ON s.plan_id = p.plan_id
INNER JOIN (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        SUM(quantity) AS total_quantity
    FROM
        ORDER_ITEMS
    GROUP BY order_id
) sub ON o.order_id = sub.order_id
WHERE
    o.order_date >= DATEADD('year', -1, CURRENT_DATE());

UPDATE FIL.ORDER_FULFILLMENT_METRICS
SET last_refreshed = CURRENT_TIMESTAMP();

ALTER TABLE FIL.ORDER_FULFILLMENT_METRICS ADD COLUMN fulfillment_notes VARCHAR(30);

CREATE OR REPLACE VIEW vz_apps.vw_top_orders AS
SELECT * FROM (
    SELECT
        store_id,
        order_id,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY total_amount) AS rn
    FROM ORDERS
) WHERE rn = 1;
