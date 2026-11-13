-- ============================================================
-- Mart: Delivery Performance
-- ============================================================
-- Analyzes delivery times by restaurant and city

WITH delivered_orders AS (
    SELECT
        restaurant_id,
        restaurant_name,
        delivery_city,
        delivery_minutes,
        order_hour,
        order_date,
        total_price
    FROM {{ ref('stg_orders') }}
    WHERE status = 'delivered'
      AND delivery_minutes IS NOT NULL
),

restaurant_stats AS (
    SELECT
        restaurant_name,
        delivery_city,
        COUNT(*) AS delivered_count,
        ROUND(AVG(delivery_minutes)::NUMERIC, 1) AS avg_delivery_min,
        ROUND(MIN(delivery_minutes)::NUMERIC, 1) AS fastest_delivery_min,
        ROUND(MAX(delivery_minutes)::NUMERIC, 1) AS slowest_delivery_min,

        -- Percentile for delivery time
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_minutes)::NUMERIC,
            1
        ) AS median_delivery_min,

        ROUND(
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delivery_minutes)::NUMERIC,
            1
        ) AS p95_delivery_min,

        ROUND(SUM(total_price)::NUMERIC, 2) AS total_revenue
    FROM delivered_orders
    GROUP BY restaurant_name, delivery_city
)

SELECT
    *,

    -- Rank restaurants by average delivery time (faster = better)
    RANK() OVER (ORDER BY avg_delivery_min ASC) AS speed_rank,

    -- Flag slow restaurants (avg > 45 min)
    CASE
        WHEN avg_delivery_min > 45 THEN 'Slow'
        WHEN avg_delivery_min > 30 THEN 'Average'
        ELSE 'Fast'
    END AS speed_tier

FROM restaurant_stats
ORDER BY avg_delivery_min ASC
