select
    o.order_id
from {{ ref('fact_Ecom1_Orders')}} o
join {{ ref('fact_Ecom1_Payments')}} p on o.order_id = p.order_id
where p.payment_type is null or p.payment_value < 0