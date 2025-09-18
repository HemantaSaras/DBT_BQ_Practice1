SELECT
  order_id,
  product_id,
  seller_id,
  price,
  shipping_charges
FROM
  {{ source('ecom1', "Ecom1_OrderItems")}}