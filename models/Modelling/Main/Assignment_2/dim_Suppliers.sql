select
    supplier_id,
    supplier_name,
    country
from {{ ref("stg_Suppliers")}}