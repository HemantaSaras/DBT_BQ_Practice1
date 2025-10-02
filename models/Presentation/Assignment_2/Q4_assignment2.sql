with orders as (
    select
        customer_id,
        count(sale_id) as total_orders
    from {{ ref("fact_Sales") }}
    group by customer_id
),

reviews as (
    select
        customer_id,
        count(review_id) as total_reviews
    from {{ ref("fact_Reviews") }}
    group by customer_id
),

loyalty as (
    select
        o.customer_id,
        o.total_orders,
        coalesce(r.total_reviews, 0) as total_reviews,
        o.total_orders + coalesce(r.total_reviews, 0) as loyalty_score
    from orders o
    left join reviews r on o.customer_id = r.customer_id
)

select *
from loyalty
where loyalty_score > 1 and total_orders > 1
order by loyalty_score desc
