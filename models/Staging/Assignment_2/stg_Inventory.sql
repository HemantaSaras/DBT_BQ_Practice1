SELECT
    inventory_id,
    product_id,
    stock_quantity,
    last_updated
from 
    {{ source('job2', 'Inventory')}}