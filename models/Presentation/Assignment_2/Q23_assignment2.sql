with customer_purchases as (
    select
        s.customer_id,
        count(distinct s.sale_id) as total_orders,
        case 
            when max(sh.shipping_status) = 'Delayed' then 'Delayed'
            else 'On-Time'
        end as shipping_experience,
        max(case when sh.shipping_status = 'Delayed' then 1 else 0 end) as had_delay
    from {{ ref('fact_Sales') }} s
    left join {{ ref('fact_Shipping') }} sh
        on s.sale_id = sh.sale_id
    group by s.customer_id
),

repeat_after_delay as (
    select
        s.customer_id,
        count(distinct s.sale_id) as repeat_orders_after_delay
    from {{ ref('fact_Sales') }} s
    where s.customer_id in (
        select customer_id 
        from customer_purchases 
        where had_delay = 1
    )
    group by s.customer_id
    having count(distinct s.sale_id) > 1
)

select
    cp.shipping_experience,
    count(distinct cp.customer_id) as num_customers,
    round(avg(cp.total_orders), 2) as avg_orders_per_customer,
    count(distinct case when cp.had_delay = 1 and ra.repeat_orders_after_delay > 1 then cp.customer_id end) 
        as repeat_customers_after_delay
from customer_purchases cp
left join repeat_after_delay ra 
    on cp.customer_id = ra.customer_id
group by cp.shipping_experience
order by avg_orders_per_customer desc

-- repeat customer after orders got delayed
