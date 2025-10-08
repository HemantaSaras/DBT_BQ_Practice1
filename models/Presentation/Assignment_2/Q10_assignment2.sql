select
    customer_location,
    avg(delivery_date - shipping_date) as avg_shipping_time
from {{ ref('fact_Shipping') }}
where shipping_status = 'Delivered'
group by customer_location


-- only delivered status avg shipping time