SELECT
    campaign_id,
    campaign_name,
    channel,
    start_date,
    end_date,
    budget
from
    {{source ('job2', 'Marketing_Campaigns')}}