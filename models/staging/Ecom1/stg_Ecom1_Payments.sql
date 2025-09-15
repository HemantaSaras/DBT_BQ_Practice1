SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM
--   `delta-coil-468606-e1.Practice_Saras.Ecom1_Payments`
  {{ source('ecom1', "Ecom1_Payments")}}
WHERE
  order_id IS NOT NULL;
