with website_session as (
    select
        customer_id,
        max(session_date) as last_session_date,
        sum(session_duration) as total_session_duration,
        sum(pages_viewed) as total_pages_viewed
    from {{ ref("stg_Website_Sessions")}}
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.location,
    c.signup_date,
    ws.last_session_date,
    coalesce(ws.total_session_duration, 0) as total_session_duration,
    coalesce(ws.total_pages_viewed, 0) as total_pages_viewed
from {{ ref("stg_Customers")}} c
left join website_session ws on c.customer_id = ws.customer_id
