{{ config(
    materialized='incremental',
    unique_key=['click_id'],
    incremental_strategy='merge'
) }}

with sessions as (
    select
        customer_id,
        max(session_date) as last_session_date,
        avg(session_duration) as avg_session_duration,
        avg(pages_viewed) as avg_pages_viewed
    from {{ ref("stg_Website_Sessions") }}
    group by customer_id
)
select
    cc.click_id,
    cc.campaign_id,
    cc.customer_id,
    cc.click_date,
    mc.campaign_name,
    mc.channel,
    s.last_session_date,
    s.avg_session_duration,
    s.avg_pages_viewed
from {{ ref("stg_Campaign_Clicks")}} cc
left join {{ ref("dim_Marketing_Campaigns")}} mc 
    on mc.campaign_id = cc.campaign_id
left join sessions s 
    on s.customer_id = cc.customer_id
