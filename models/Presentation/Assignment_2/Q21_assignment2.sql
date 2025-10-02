select
    customer_location,
    count(distinct customer_id) as total_customers,
    sum(session_duration) as total_session_duration,
    sum(pages_viewed) as total_pages_viewed,
    round(avg(session_duration), 2) as avg_session_duration_per_session,
    round(avg(pages_viewed), 2) as avg_pages_per_session
from {{ ref('fact_Website_Sessions') }}
group by customer_location
order by total_session_duration desc
