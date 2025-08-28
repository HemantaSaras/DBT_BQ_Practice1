 select 
       id as payment_id,
       orderid as order_id,
       paymentmethod as payment_method,
       status
       --amount convert cents to dollar
       {{cents_to_dollar('amount',4)}} as amount,
         created as created_at
    from {{ source('stripe', 'jaffle_payment') }}
