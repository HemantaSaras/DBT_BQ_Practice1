select 
    date(o.order_purchase_timestamp) as order_date,
    p.product_category_name as product_name,
    count(oi.product_id) as quantity,
    oi.item_price as item_price,
    o.paid_amount as payment_fee,
    o.paid_amount as total_amount_per_order
from {{ ref('fact_Ecom1_Orders') }} o
join {{ ref('fact_Ecom1_OrderItems') }} oi on o.order_id = oi.order_id
join {{ ref('dim_Ecom1_Products') }} p on oi.product_id = p.product_id
where o.customer_id = 'BEQKIEZWAQJO'
group by 1,2,oi.item_price,o.paid_amount

