with supplier_revenue as (
    select
        p.supplier_id,
        sum(fs.product_price * fs.quantity) as revenue_generated
    from {{ ref('fact_Sales') }} fs
    left join {{ ref('dim_Products') }} p
        on fs.product_id = p.product_id
    group by p.supplier_id
),

total_marketing as (
    select
        sum(budget) as marketing_spent
    from {{ ref('dim_Marketing_Campaigns') }}
)

select
    r.supplier_id,
    r.revenue_generated,
    t.marketing_spent,
    round(r.revenue_generated * 100 / nullif(t.marketing_spent,0),2) as roi_percentage
from supplier_revenue r
cross join total_marketing t
left join {{ ref('dim_Suppliers') }} sup
    on r.supplier_id = sup.supplier_id
order by roi desc
