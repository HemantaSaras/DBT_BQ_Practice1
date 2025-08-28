with customers as (
    select *
    from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select *
    from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select *
    from {{ ref('stg_jaffle_shop__payment') }}
),


customer_orders as (
    select
        USER_ID as customer_id,
        count(ID) as total_orders
    from orders
    group by USER_ID
),

customer_payments as (
    select
        o.USER_ID as customer_id,
        sum(p.amount) as total_amount
    from payments p
    join orders o on p.orderid = o.ID
    group by o.USER_ID
)


select
    c.ID as customer_id,
    c.FIRST_NAME as first_name,
    c.LAST_NAME as last_name,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(p.total_amount, 0) as total_amount
from customers c
left join customer_orders o on c.ID = o.customer_id
left join customer_payments p on c.ID = p.customer_id