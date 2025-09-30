with inventory as (
    select
        product_id,
        sum(stock_quantity) as total_stock
    from {{ ref("stg_Inventory")}}
    group by product_id
),
avg_reviews as (
    select
        product_id,
        avg(rating) as avg_rating
    from {{ ref("stg_Reviews")}}
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.category as product_category,
    p.price,
    coalesce(i.total_stock, 0) as total_stock,
    coalesce(a.avg_rating, 0) as avg_rating
from {{ ref("stg_Products")}} p
left join inventory i on p.product_id = i.product_id
left join avg_reviews a on p.product_id = a.product_id
