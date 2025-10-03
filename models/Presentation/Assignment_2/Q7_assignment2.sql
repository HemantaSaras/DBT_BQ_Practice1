select
    s.supplier_id,
    sum(fs.product_price * fs.quantity) as total_revenue
from {{ ref('fact_Sales') }} fs
left join {{ ref('dim_Products') }} s
    on fs.product_id = s.product_id
left join {{ ref('dim_Suppliers') }} sup
    on s.supplier_id = sup.supplier_id
group by s.supplier_id
order by total_revenue desc
