select
    product_id,
    product_name
from {{ ref('dim_Products') }}
where total_stock >= 20