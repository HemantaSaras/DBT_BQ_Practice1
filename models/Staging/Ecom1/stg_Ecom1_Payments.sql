SELECT
  order_id,
  payment_sequential,
  payment_type,
  COALESCE(payment_installments, 0) as payment_installments,
  payment_value
FROM
  {{ source('ecom1', "Ecom1_Payments")}}
WHERE
  order_id IS NOT NULL
