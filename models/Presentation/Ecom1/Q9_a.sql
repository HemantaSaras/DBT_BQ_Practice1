with product_pairs as (
    select
        oi1.product_id as product_a,
        oi2.product_id as product_b,
        count(distinct oi1.order_id) as times_purchased_together
    from {{ ref('fact_Ecom1_OrderItems') }} oi1
    join {{ ref('fact_Ecom1_OrderItems') }} oi2
        on oi1.order_id = oi2.order_id
       and oi1.product_id < oi2.product_id
    group by oi1.product_id, oi2.product_id
),

ranked_pairs as (
    select
        product_a,
        product_b,
        times_purchased_together,
        dense_rank() over (order by times_purchased_together desc) as pair_rank
    from product_pairs
)

select
    product_a,
    product_b,
    times_purchased_together
from ranked_pairs
where pair_rank <= 5
order by times_purchased_together desc
