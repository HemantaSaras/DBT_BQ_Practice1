-- {% set stg_orders = ref('stg_Ecom1_Orders') %}

{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id', 'seller_id'],
    incremental_strategy='merge'
) }}

WITH items AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(seller_id AS STRING) AS seller_id,
        CAST(price AS NUMERIC) AS item_price,
        CAST(shipping_charges AS NUMERIC) AS shipping_charges,
    FROM {{ ref('stg_Ecom1_OrderItems') }}
    {% if is_incremental() %}
      WHERE order_id IN (SELECT DISTINCT order_id FROM {{ ref('stg_Ecom1_Orders') }})
    {% endif %}
)

SELECT
    i.order_id,
    i.product_id,
    i.seller_id,
    i.item_price,
    i.shipping_charges,
FROM items i
