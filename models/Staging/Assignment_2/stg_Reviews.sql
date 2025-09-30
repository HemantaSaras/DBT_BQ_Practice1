select
    review_id,
    customer_id,
    product_id,
    rating,
    review_date
from 
    {{ source("job2", "Reviews")}}