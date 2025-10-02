with customers_with_campaign as (
    select
        cc.campaign_id,
        cc.campaign_name,
        count(distinct cc.customer_id) as total_customers_clicked
    from {{ ref('fact_Campaign_Clicks') }} cc
    group by cc.campaign_id, cc.campaign_name
),

customers_purchased as (
    select
        cc.campaign_id,
        count(distinct s.customer_id) as purchased_customers
    from {{ ref('fact_Campaign_Clicks') }} cc
    inner join {{ ref('fact_Sales') }} s 
        on cc.customer_id = s.customer_id
    group by cc.campaign_id
)

select
    c.campaign_id,
    c.campaign_name,
    c.total_customers_clicked,
    coalesce(p.purchased_customers, 0) as purchased_customers,
    round(coalesce(p.purchased_customers, 0) * 100.0 / nullif(c.total_customers_clicked,0), 2) as conversion_rate_pct
from customers_with_campaign c
left join customers_purchased p on c.campaign_id = p.campaign_id

