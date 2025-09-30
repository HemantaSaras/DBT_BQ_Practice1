select
    session_id,
    customer_id,
    session_date,
    device,
    session_duration,
    pages_viewed
FROM 
    {{ source("job2", "Website_Sessions")}}