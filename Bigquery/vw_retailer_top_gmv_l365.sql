SELECT  
  merchant_id
  ,merchant_key
  ,gmv_l365
FROM `kiotvietplus.merchant_profile_warehouse.merchant_transaction` 
ORDER BY gmv_l365 DESC
LIMIT 100