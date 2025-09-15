SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_timestamp,
  order_estimated_delivery_date
FROM
  `delta-coil-468606-e1.Practice_Saras.Ecom1_Orders`
WHERE
  order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND NOT ( order_status IS NULL
    AND order_purchase_timestamp IS NULL
    AND order_approved_at IS NULL
    AND order_delivered_timestamp IS NULL
    AND order_estimated_delivery_date IS NULL)