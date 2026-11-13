-- ============================================================
-- Custom dbt test: Delivered orders must have a delivered_at timestamp
-- ============================================================

SELECT
    order_id,
    status,
    delivered_at
FROM {{ ref('stg_orders') }}
WHERE status = 'delivered'
  AND delivered_at IS NULL
