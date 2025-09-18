  {{ config( MATERIALIZED='table' ) }}
WITH
  customers AS (
  SELECT
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
  FROM
    {{ ref('stg_Ecom1_Customers') }} ),
    
  first_order AS (
  SELECT
    o.customer_id,
    MIN(o.order_purchase_timestamp) AS first_order_ts,
    COUNT(DISTINCT o.order_id) AS total_orders
  FROM
    {{ ref('stg_Ecom1_Orders') }} o
  GROUP BY
    1 )
SELECT
  c.customer_id,
  c.customer_zip_code_prefix,
  c.customer_city,
  c.customer_state,
  f.first_order_ts,
  f.total_orders,
  CASE
    WHEN f.total_orders > 1 THEN TRUE
    ELSE FALSE
END
  AS is_repeat_customer
FROM
  customers c
LEFT JOIN
  first_order f
ON
  f.customer_id = c.customer_id