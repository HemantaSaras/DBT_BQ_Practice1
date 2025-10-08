select
    s.customer_id,
    count(distinct s.product_category) as number_of_categories
from {{ ref('fact_Sales') }} s 
group by s.customer_id
having number_of_categories > 1
