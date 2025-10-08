with campaign_sales as (
    select
        cc.campaign_id,
        cc.campaign_name,
        sum(s.product_price * s.quantity) as total_revenue
    from {{ ref('fact_Campaign_Clicks') }} cc
    join {{ ref('fact_Sales') }} s
        on cc.customer_id = s.customer_id and date(cc.click_date) = date(s.sale_date)
    group by cc.campaign_id, cc.campaign_name
)

select *
from campaign_sales
qualify row_number() over (order by total_revenue desc) = 1

-- for that same campaign date 