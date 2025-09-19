{{ config(materialized='view') }}

select
    o.customer_id,
    o.order_id,
    date(o.order_purchase_timestamp) as order_date,
    lag(date(o.order_purchase_timestamp)) over (
        partition by o.customer_id
        order by o.order_purchase_timestamp
    ) as prev_order_date,
    date_diff(
        date(o.order_purchase_timestamp),
        lag(date(o.order_purchase_timestamp)) over (
            partition by o.customer_id
            order by o.order_purchase_timestamp
        ),
        day
    ) as days_between_orders
from {{ ref('fact_Ecom1_Orders') }} o
qualify lag(o.order_purchase_timestamp) over (
    partition by o.customer_id
    order by o.order_purchase_timestamp
) is not null
order by o.customer_id, order_date
