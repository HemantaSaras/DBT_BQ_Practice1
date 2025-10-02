with customer_pages as (
    select
        customer_id,
        sum(pages_viewed) as total_pages_viewed
    from {{ ref('fact_Website_Sessions') }}
    group by customer_id
),

customer_spend as (
    select
        customer_id,
        sum(product_price * quantity) as total_spent
    from {{ ref('fact_Sales') }}
    group by customer_id
)

select
    p.customer_id,
    p.total_pages_viewed,
    coalesce(s.total_spent, 0) as total_spent
from customer_pages p
left join customer_spend s
    on p.customer_id = s.customer_id
order by total_pages_viewed desc
