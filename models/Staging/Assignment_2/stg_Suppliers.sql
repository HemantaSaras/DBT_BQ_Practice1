select
    supplier_id,
    supplier_name,
    country
from
    {{ source("job2", "Suppliers")}}