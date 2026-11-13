-- ============================================================
-- Mart: Daily Revenue Report
-- ============================================================
-- Aggregated daily metrics with running totals (window function)

WITH daily AS (
    SELECT
        order_date,
        COUNT(*) AS total_orders,
        SUM(total_price) AS revenue,
        AVG(total_price) AS avg_order_value,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(quantity) AS items_sold
    FROM {{ ref('stg_orders') }}
    GROUP BY order_date
)

SELECT
    order_date,
    total_orders,
    ROUND(revenue::NUMERIC, 2) AS revenue,
    ROUND(avg_order_value::NUMERIC, 2) AS avg_order_value,
    unique_customers,
    items_sold,

    -- Window functions: running totals and comparisons
    SUM(revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,

    LAG(revenue) OVER (ORDER BY order_date) AS prev_day_revenue,

    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY order_date))
        / NULLIF(LAG(revenue) OVER (ORDER BY order_date), 0) * 100,
        2
    ) AS revenue_change_pct,

    -- Ranking days by revenue
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank

FROM daily
ORDER BY order_date DESC
