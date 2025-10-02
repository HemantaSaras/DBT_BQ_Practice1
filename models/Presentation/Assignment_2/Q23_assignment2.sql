with customer_purchases as (
    select
        s.customer_id,
        count(distinct s.sale_id) as total_orders,
        case 
            when max(sh.shipping_status) = 'Delayed' then 'Delayed'
            else 'On-Time'
        end as shipping_experience
    from {{ ref('fact_Sales') }} s
    left join {{ ref('fact_Shipping') }} sh
        on s.sale_id = sh.sale_id
    group by s.customer_id
)

select
    shipping_experience,
    count(distinct customer_id) as num_customers,
    round(avg(total_orders), 2) as avg_orders_per_customer
from customer_purchases
group by shipping_experience
order by avg_orders_per_customer desc
