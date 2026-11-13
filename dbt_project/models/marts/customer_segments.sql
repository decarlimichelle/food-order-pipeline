-- ============================================================
-- Mart: Customer Segmentation
-- ============================================================
-- Groups customers by ordering behavior using CTEs and
-- window functions. Assigns segments based on frequency
-- and spend.

WITH customer_metrics AS (
    SELECT
        customer_id,
        customer_name,
        delivery_city,
        COUNT(*) AS total_orders,
        SUM(total_price) AS total_spent,
        AVG(total_price) AS avg_order_value,
        MIN(order_timestamp) AS first_order,
        MAX(order_timestamp) AS last_order,
        COUNT(DISTINCT order_date) AS active_days,
        COUNT(DISTINCT item_id) AS unique_items_ordered
    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    GROUP BY customer_id, customer_name, delivery_city
),

percentiles AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY total_spent) AS spend_quartile,
        NTILE(4) OVER (ORDER BY total_orders) AS frequency_quartile
    FROM customer_metrics
)

SELECT
    customer_id,
    customer_name,
    delivery_city,
    total_orders,
    ROUND(total_spent::NUMERIC, 2) AS total_spent,
    ROUND(avg_order_value::NUMERIC, 2) AS avg_order_value,
    first_order,
    last_order,
    active_days,
    unique_items_ordered,
    spend_quartile,
    frequency_quartile,

    -- Customer segment based on quartiles
    CASE
        WHEN spend_quartile = 4 AND frequency_quartile = 4 THEN 'VIP'
        WHEN spend_quartile >= 3 AND frequency_quartile >= 3 THEN 'Loyal'
        WHEN spend_quartile >= 2 OR frequency_quartile >= 2 THEN 'Regular'
        ELSE 'New / Low Activity'
    END AS customer_segment,

    -- Days since last order
    EXTRACT(DAY FROM NOW() - last_order) AS days_since_last_order

FROM percentiles
ORDER BY total_spent DESC
