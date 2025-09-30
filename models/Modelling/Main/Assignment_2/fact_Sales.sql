{{ config(
    materialized= "incremental",
    unique_key= ['sale_id'],
    incremental_strategy= "merge",
    partition_by={
        "field": "sale_date",
        "data_type": "date"
    }
) }}

with customers as (
    select
        customer_id,
        customer_name,
        location as customer_location,
        signup_date as customer_signup_date
    from {{ ref("dim_Customers") }}
),

products as (
    select
        product_id,
        product_name,
        product_category,
        price as product_price,            
        total_stock,
        avg_rating
    from {{ ref("dim_Products") }}
)

select
    s.sale_id,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.sale_date,
    c.customer_name,
    c.customer_location,
    c.customer_signup_date,
    p.product_name,
    p.product_category,
    p.product_price,            
    p.total_stock,
    p.avg_rating
from {{ ref("stg_Sales")}} s
left join customers c on c.customer_id = s.customer_id
left join products p on p.product_id = s.product_id
