with last_month_orders as (
    select
        o.customer_id,
        count(distinct o.order_id) as order_count_last_month
    from {{ ref('fact_Ecom1_Orders') }} o
    where date_trunc(date(o.order_purchase_timestamp), month) 
          = date_trunc(date_sub(current_date(), interval 1 month), month)
    group by o.customer_id
)

select
    count(*) as total_repeat_customers_last_month
from last_month_orders
where order_count_last_month > 1
