select
    count(distinct oi.order_id) as bad_orders_count
from {{ ref('fact_Ecom1_OrderItems') }} oi
where oi.item_price <= 0
