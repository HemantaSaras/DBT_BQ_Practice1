{{ config(
    materialized= "incremental",
    unique_key= ['shipping_id'],
    incremental_strategy= "merge",
    partition_by={
        "field": "shipping_date",
        "data_type": "date"
    }
)}}


with sales as (
    select
        sale_id,
        customer_id,
        product_id,
        quantity,
        sale_date,
        customer_name,
        customer_location,
        customer_signup_date,
        product_name,
        product_category,
        product_price,            
        total_stock,
        avg_rating
    from {{ ref("fact_Sales")}} s
)


SELECT
    sh.shipping_id,
    sh.sale_id,
    sh.shipping_date,
    sh.delivery_date,
    sh.shipping_cost,
    sh.shipping_status,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.sale_date,
    s.customer_name,
    s.customer_location,
    s.customer_signup_date,
    s.product_name,
    s.product_category,
    s.product_price,            
    s.total_stock,
    s.avg_rating
from {{ ref("stg_Shipping")}} sh
left join sales s on s.sale_id = sh.sale_id