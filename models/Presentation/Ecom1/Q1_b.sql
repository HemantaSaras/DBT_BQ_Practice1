select
    o.order_id,
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    p.payment_type,
    p.payment_value,
    c.is_repeat_customer
from {{ ref('fact_Ecom1_Orders')}} o 
join {{ ref('dim_Ecom1_Customers')}} c on o.customer_id = c.customer_id
join {{ ref('fact_Ecom1_Payments')}} p on o.order_id = p.order_id
