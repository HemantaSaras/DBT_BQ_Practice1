select distinct
    o.customer_id,
    p.product_id,
    p.product_category_name
from {{ ref('fact_Ecom1_Orders') }} o
join {{ ref('fact_Ecom1_OrderItems') }} oi on o.order_id = oi.order_id
join {{ ref('dim_Ecom1_Products') }} p on oi.product_id = p.product_id
order by o.customer_id, p.product_id