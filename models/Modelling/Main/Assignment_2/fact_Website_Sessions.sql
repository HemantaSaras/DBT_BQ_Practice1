{{ config(
    materialized= "incremental",
    unique_key= ['session_id'],
    incremental_strategy= "merge",
    partition_by={
        "field": "session_date",
        "data_type": "date"
    }
) }}

with customers as (
    select
        customer_id,
        customer_name,
        location as customer_location,
        signup_date as customer_signup_date
    from {{ ref("dim_Customers") }}
)

select
    ws.session_id,
    ws.customer_id,
    ws.session_date,
    ws.device,
    ws.session_duration,
    ws.pages_viewed,
    c.customer_name,
    c.customer_location,
    c.customer_signup_date
from {{ ref("stg_Website_Sessions") }} ws
left join customers c on c.customer_id = ws.customer_id
