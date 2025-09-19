select
    oi.product_id,
    count(*) as number_of_times_appear,
    rank() over (order by count(*) desc) as product_rank
from {{ ref('fact_Ecom1_Orders') }} o
join {{ ref('fact_Ecom1_OrderItems') }} oi on o.order_id = oi.order_id
join {{ ref('dim_Ecom1_Customers')}} c on o.customer_id = c.customer_id
group by oi.product_id
order by number_of_times_appear desc