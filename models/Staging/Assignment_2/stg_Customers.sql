SELECT
    customer_id,
    customer_name,
    location,
    signup_date
FROM
    {{source ('job2', 'Customers')}}