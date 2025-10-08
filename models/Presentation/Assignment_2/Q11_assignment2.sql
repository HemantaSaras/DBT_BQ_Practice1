with status_avg as (
    select
        sh.shipping_id,
        sh.shipping_status,
        avg(r.rating) as avg_rating,
        count(*) as num_reviews
    from {{ ref('fact_Shipping') }} sh
    left join {{ ref('fact_Reviews') }} r
        on sh.customer_id = r.customer_id
       and sh.product_id = r.product_id
    group by sh.shipping_status, shipping_id
),

overall_avg as (
    select avg(rating) as overall_rating
    from {{ ref('fact_Reviews') }}
)

select
    s.shipping_id,
    s.shipping_status,
    s.avg_rating,
    s.num_reviews
from status_avg s
cross join overall_avg o
where s.avg_rating < o.overall_rating
order by s.avg_rating


-- which shipping id is related to lower ratings