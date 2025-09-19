select
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    c.first_order_ts,
    c.total_orders
from {{ ref('dim_Ecom1_Customers') }} c
where c.first_order_ts >= timestamp_sub(current_timestamp(), interval 30 day)