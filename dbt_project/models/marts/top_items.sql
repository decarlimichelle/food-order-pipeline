-- ============================================================
-- Mart: Top Items Analysis
-- ============================================================
-- Ranks items by popularity with revenue contribution percentage

WITH item_stats AS (
    SELECT
        item_id,
        item_name,
        item_category,
        unit_price,
        COUNT(*) AS times_ordered,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_buyers,
        AVG(quantity) AS avg_qty_per_order
    FROM {{ ref('stg_orders') }}
    GROUP BY item_id, item_name, item_category, unit_price
),

totals AS (
    SELECT SUM(total_revenue) AS grand_total_revenue
    FROM item_stats
)

SELECT
    s.item_name,
    s.item_category,
    s.unit_price,
    s.times_ordered,
    s.total_quantity,
    ROUND(s.total_revenue::NUMERIC, 2) AS total_revenue,
    s.unique_buyers,
    ROUND(s.avg_qty_per_order::NUMERIC, 1) AS avg_qty_per_order,

    -- Revenue contribution percentage
    ROUND(
        (s.total_revenue / NULLIF(t.grand_total_revenue, 0) * 100)::NUMERIC,
        2
    ) AS revenue_pct,

    -- Rank within category
    ROW_NUMBER() OVER (
        PARTITION BY s.item_category
        ORDER BY s.total_revenue DESC
    ) AS rank_in_category,

    -- Overall rank
    RANK() OVER (ORDER BY s.total_revenue DESC) AS overall_rank

FROM item_stats s
CROSS JOIN totals t
ORDER BY total_revenue DESC
