with clicks as (
    select count(distinct customer_id) as num_clicked
    from {{ ref('fact_Campaign_Clicks') }}
),

sessions as (
    select count(distinct customer_id) as num_sessions
    from {{ ref('fact_Website_Sessions') }}
),

sales as (
    select count(distinct customer_id) as num_sales
    from {{ ref('fact_Sales') }}
),

payments as (
    select count(distinct customer_id) as num_payments
    from {{ ref('fact_Payments') }}
    where payment_status = 'Success'
),

reviews as (
    select count(distinct customer_id) as num_reviews
    from {{ ref('fact_Reviews') }}
)

select
    c.num_clicked,
    s.num_sessions,
    sa.num_sales,
    p.num_payments,
    r.num_reviews,
    round(sa.num_sales * 100.0 / nullif(c.num_clicked,0),2) as sales_conversion_pct,
    round(p.num_payments * 100.0 / nullif(sa.num_sales,0),2) as payment_conversion_pct,
    round(r.num_reviews * 100.0 / nullif(p.num_payments,0),2) as review_conversion_pct
from clicks c
cross join sessions s
cross join sales sa
cross join payments p
cross join reviews r
