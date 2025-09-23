{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

WITH orders AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_timestamp,
        order_estimated_delivery_date
    FROM {{ ref('stg_Ecom1_Orders') }}
    {% if is_incremental() %}
      WHERE order_purchase_timestamp > (
        SELECT COALESCE(MAX(order_purchase_timestamp), TIMESTAMP('1900-01-01')) 
        FROM {{ this }}
      )
    {% endif %}
),

item_rollup AS (
    SELECT
        oi.order_id,
        COUNT(*) AS item_count,
        SUM(oi.item_price) AS item_subtotal,
        SUM(oi.shipping_charges) AS shipping_total
    FROM {{ ref('fact_Ecom1_OrderItems') }} oi
    GROUP BY oi.order_id
),

payment_rollup AS (
    SELECT
        p.order_id,
        SUM(p.payment_value) AS paid_amount
    FROM {{ ref('fact_Ecom1_Payments') }} p
    GROUP BY p.order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_timestamp,
    o.order_estimated_delivery_date,
    ir.item_count,
    ir.item_subtotal, -- item_price
    ir.shipping_total,
    (ir.item_subtotal + COALESCE(ir.shipping_total, 0)) AS merchandise_plus_shipping,
    pr.paid_amount,
    (ir.item_subtotal + COALESCE(ir.shipping_total, 0)) AS order_total,
    CASE
        WHEN pr.paid_amount >= (ir.item_subtotal + COALESCE(ir.shipping_total, 0)) THEN TRUE
        ELSE FALSE
    END AS is_fully_paid
FROM orders o
LEFT JOIN item_rollup ir ON ir.order_id = o.order_id
LEFT JOIN payment_rollup pr ON pr.order_id = o.order_id
