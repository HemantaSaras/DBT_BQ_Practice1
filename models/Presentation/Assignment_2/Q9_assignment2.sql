select
    countif(sh.shipping_status = 'Delayed') * 100.0 / count(*) as pct_delayed_orders
from {{ ref("fact_Shipping") }} sh
