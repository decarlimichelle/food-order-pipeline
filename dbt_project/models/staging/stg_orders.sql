-- ============================================================
-- Staging: Clean and standardize raw orders
-- ============================================================
-- This model reads from the fact_orders table and applies
-- basic cleaning: filters out test data, standardizes fields.

WITH source AS (
    SELECT
        order_id,
        customer_id,
        restaurant_id,
        item_id,
        quantity,
        total_price,
        status,
        order_timestamp,
        delivered_at,
        delivery_city
    FROM {{ source('raw', 'fact_orders') }}
    WHERE status IS NOT NULL
      AND total_price > 0
),

enriched AS (
    SELECT
        s.*,
        c.customer_name,
        c.email AS customer_email,
        i.item_name,
        i.category AS item_category,
        i.unit_price,
        r.restaurant_name,
        r.cuisine_type
    FROM source s
    JOIN {{ source('raw', 'dim_customers') }} c ON s.customer_id = c.customer_id
    JOIN {{ source('raw', 'dim_items') }} i ON s.item_id = i.item_id
    JOIN {{ source('raw', 'dim_restaurants') }} r ON s.restaurant_id = r.restaurant_id
)

SELECT
    order_id,
    customer_id,
    customer_name,
    customer_email,
    restaurant_id,
    restaurant_name,
    cuisine_type,
    item_id,
    item_name,
    item_category,
    unit_price,
    quantity,
    total_price,
    status,
    order_timestamp,
    delivered_at,
    delivery_city,

    -- Calculated fields
    EXTRACT(HOUR FROM order_timestamp) AS order_hour,
    EXTRACT(DOW FROM order_timestamp) AS order_day_of_week,
    DATE(order_timestamp) AS order_date,

    -- Delivery duration (only for delivered orders)
    CASE
        WHEN delivered_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (delivered_at - order_timestamp)) / 60.0
        ELSE NULL
    END AS delivery_minutes

FROM enriched
