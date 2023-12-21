WITH SORT_GMV AS (
  SELECT  
    -- merchant_id
    merchant_key
    ,ROW_NUMBER() OVER (PARTITION BY r.industry_key ORDER BY gmv_l365 DESC) AS rn
    -- ,gmv_l365
  FROM `kiotvietplus.merchant_profile_warehouse.merchant_transaction` AS mt 
  JOIN  `kiotvietplus.kv_datawarehouse.retailer` AS r ON mt.merchant_key = r.retailer_key
)

SELECT merchant_key FROM SORT_GMV
WHERE rn < 50
