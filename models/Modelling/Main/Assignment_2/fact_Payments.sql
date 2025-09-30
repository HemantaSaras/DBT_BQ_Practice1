{{ config(
    materialized= "incremental",
    unique_key= ['payment_id'],
    incremental_strategy= 'merge'
) }}

with sales as (
    select
        sale_id,
        customer_id,
        product_id,
        quantity,
        sale_date
    from {{ ref("stg_Sales") }}
),

customers as (
    select
        customer_id,
        customer_name,
        location,
        signup_date
    from {{ ref("dim_Customers") }}
)

select
    p.payment_id,
    p.sale_id,
    p.payment_method,
    p.payment_status,
    p.payment_date,
    p.amount,
    s.customer_id,
    c.customer_name,
    c.location,
    c.signup_date,
    s.product_id,
    s.quantity,
    s.sale_date
from {{ ref("stg_Payments") }} p
left join sales s on p.sale_id = s.sale_id
left join customers c on s.customer_id = c.customer_id
