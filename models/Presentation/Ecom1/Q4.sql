with monthly_avg as (
    select
        format_date('%Y-%m', date(o.order_purchase_timestamp)) as order_month,
        avg(o.order_total) as avg_order_value
    from {{ ref('fact_Ecom1_Orders') }} o
    group by format_date('%Y-%m', date(o.order_purchase_timestamp))
)

select
    order_month,
    avg_order_value
from monthly_avg
where avg_order_value > 200
order by order_month