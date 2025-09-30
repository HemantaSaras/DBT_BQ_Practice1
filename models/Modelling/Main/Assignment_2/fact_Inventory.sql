{{ config(
    materialized='incremental',
    unique_key=['inventory_id'],
    incremental_strategy='merge'
) }}

with inventory as (
    select
        inventory_id,
        product_id,
        stock_quantity,
        last_updated,
        case
            when coalesce(stock_quantity, 0) > 0 then true
            else false
        end as stock_available
    from {{ ref("stg_Inventory") }}
),

product_details as (
    select
        product_id,
        product_name,
        product_category,
        price
    from {{ ref("dim_Products") }}
)

select
    i.inventory_id,
    i.product_id,
    p.product_name,
    p.product_category,
    p.price,
    i.stock_quantity,
    i.last_updated,
    i.stock_available
from inventory i
left join product_details p 
    on i.product_id = p.product_id
