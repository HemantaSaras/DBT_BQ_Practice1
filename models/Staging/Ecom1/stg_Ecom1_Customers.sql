SELECT
  customer_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM
  {{ source('ecom1', "Ecom1_Customers")}}