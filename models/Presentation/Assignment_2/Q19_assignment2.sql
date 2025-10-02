select
    device,
    avg(session_duration) as avg_session_duration
from {{ ref('fact_Website_Sessions') }}
group by device
order by avg_session_duration desc