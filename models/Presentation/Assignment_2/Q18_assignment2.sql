with campaign_sales as (
    select
    cc.campaign_id,
    cc.campaign_name,
    sum(s.product_price * s.quantity) as total_revenue
    from {{ ref('fact_Campaign_Clicks') }} cc
    left join {{ ref('fact_Sales') }} s on cc.customer_id = s.customer_id
    group by cc.campaign_id, cc.campaign_name
)

select
    mc.campaign_id,
    mc.campaign_name,
    cs.total_revenue,
    mc.budget,
    cs.total_revenue / mc.budget * 100 as roi_pct
from {{ ref('dim_Marketing_Campaigns') }} mc
left join campaign_sales cs on mc.campaign_id = cs.campaign_id