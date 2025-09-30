select
    mc.campaign_id,
    mc.campaign_name,
    mc.channel,
    mc.start_date,
    mc.end_date,
    mc.budget
from {{ ref("stg_Marketing_Campaigns")}} mc
