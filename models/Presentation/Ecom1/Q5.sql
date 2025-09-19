select
    o.order_id,
    o.is_fully_paid
from {{ ref('fact_Ecom1_Orders')}} o
where o.is_fully_paid = TRUE