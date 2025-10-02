select
    campaign_id,
    campaign_name,
    count(distinct customer_id)
from {{ ref('fact_Campaign_Clicks') }}
group by campaign_id, campaign_name