select
    countif(sh.shipping_status = 'Delayed') * 100.0 / count(shipping_id) as pct_delayed_orders
from {{ ref("fact_Shipping") }} sh

-- there is duplicate shipping id in delivery and delayed status,