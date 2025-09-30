{{ config(
    materialized= "incremental",
    unique_key= ['review_id'],
    incremental_strategy= "merge"
)}}

with customers as (
    select
        customer_id,
        customer_name,
        location,
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
    r.review_id,
    r.customer_id,
    r.product_id,
    r.rating,
    r.review_date,
    c.customer_name,
    c.location,
    c.customer_signup_date,                  
    p.product_name,
    p.product_category,
    p.product_price,                         
    p.total_stock,
    p.avg_rating
from {{ ref("stg_Reviews")}} r
left join customers c on c.customer_id = r.customer_id
left join products p on p.product_id = r.product_id
