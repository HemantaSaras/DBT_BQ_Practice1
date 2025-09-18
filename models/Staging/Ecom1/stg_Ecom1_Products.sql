SELECT
  product_id,
  product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM
  {{ source('ecom1', "Ecom1_Products")}}
WHERE
  product_id IS NOT NULL AND
  not(
    product_width_cm IS NULL
  AND product_height_cm IS NULL
  AND product_weight_g IS NULL
  AND product_length_cm IS null
  )
  
