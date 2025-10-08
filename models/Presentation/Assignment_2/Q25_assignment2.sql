with ordered_sales as (
    select
        customer_id,
        sale_id,
        sale_date,
        product_price * quantity as revenue,
        row_number() over (partition by customer_id order by sale_date) as order_rank
    from {{ ref('fact_Sales') }}
),

customer_type_revenue as (
    select
        case 
            when order_rank = 1 then 'New Customer'
            else 'Repeat Customer'
        end as customer_type,
        sum(revenue) as total_revenue
    from ordered_sales
    group by customer_type
)

select
    customer_type,
    total_revenue,
    round(total_revenue * 100.0 / sum(total_revenue) over (), 2) as revenue_pct
from customer_type_revenue


-- row number function on sale based on timeline