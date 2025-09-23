-- {% set stg_orders = ref('stg_Ecom1_Orders') %}

{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential'],
    incremental_strategy='merge'
) }}

SELECT
    CAST(order_id AS STRING) AS order_id,
    CAST(payment_sequential AS INT64) AS payment_sequential,
    payment_type,
    CAST(payment_installments AS INT64) AS payment_installments,
    CAST(payment_value AS NUMERIC) AS payment_value
FROM {{ ref('stg_Ecom1_Payments') }}
{% if is_incremental() %}
  WHERE order_id IN (SELECT DISTINCT order_id FROM {{ ref('stg_Ecom1_Orders') }})
{% endif %}
