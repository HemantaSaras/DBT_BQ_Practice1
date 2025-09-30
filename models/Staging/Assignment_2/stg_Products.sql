SELECT
    product_id,
    product_name,
    category,
    price
from 
    {{ source("job2", "Products")}}