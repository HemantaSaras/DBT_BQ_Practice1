SELECT
    product_id,
    product_name,
    category,
    price,
    supplier_id
from 
    {{ source("job2", "Products")}}