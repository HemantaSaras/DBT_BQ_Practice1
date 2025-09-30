SELECT
  click_id,
  campaign_id,
  customer_id,
  click_date
FROM
    {{source ('job2', 'Campaign_Clicks')}}