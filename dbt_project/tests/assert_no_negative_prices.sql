-- ============================================================
-- Custom dbt test: No negative prices in orders
-- ============================================================
-- This test fails if any rows are returned (i.e., negative prices exist)

SELECT
    order_id,
    total_price
FROM {{ ref('stg_orders') }}
WHERE total_price < 0
