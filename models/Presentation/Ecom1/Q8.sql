{{ config(materialized='view') }}

WITH repeat_customers AS (
    SELECT
        customer_id
    FROM {{ ref('dim_Ecom1_Customers') }}
    WHERE is_repeat_customer = TRUE
),

ordered AS (
    SELECT
        o.customer_id,
        o.order_purchase_timestamp AS order_date
    FROM {{ ref('stg_Ecom1_Orders') }} o
    JOIN repeat_customers c
        ON o.customer_id = c.customer_id
    WHERE o.order_purchase_timestamp IS NOT NULL
)

SELECT
    customer_id,
    order_date,
    COALESCE(
        DATE_DIFF(
            DATE(order_date),
            LAG(DATE(order_date)) OVER (PARTITION BY customer_id ORDER BY order_date),
            DAY
        ),
        0
    ) AS time_since_previous_order
FROM ordered
ORDER BY customer_id, order_date
