with customer_orders as (
    select
        customer_id,
        count(distinct sale_id) as total_orders,
        sum(product_price * quantity) as total_revenue
    from {{ ref('fact_Sales') }}
    group by customer_id
)

select
    case 
        when total_orders = 1 then 'New Customer'
        else 'Repeat Customer'
    end as customer_type,
    sum(total_revenue) as revenue,
    round(sum(total_revenue) * 100.0 / sum(sum(total_revenue)) over (), 2) as revenue_pct
from customer_orders
group by customer_type
