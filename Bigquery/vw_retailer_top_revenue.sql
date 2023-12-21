-- Người tạo: hien.nt8
-- Mục đích: lấy các gian hàng top revenue

WITH
-- Gian hàng retail đã ký HĐ và còn hạn sử dụng
re AS
    (
    SELECT DISTINCT
      CAST(retailer.real_key AS STRING) AS retailer_id 
      , retailer.industry_key
      , industry.v_industry_name  AS industry_name
      , DATE(retailer.created_date) AS created_date
      , DATE(retailer.expiry_date) AS expiry_date
    FROM `kiotvietplus.kv_datawarehouse.retailer` AS retailer
    LEFT JOIN `kiotvietplus.kv_datawarehouse.industry` AS industry ON retailer.industry_key = industry.industry_key
    LEFT JOIN `kiotvietplus.kv_datawarehouse.d_serverkey` AS serverkey ON retailer.server_key = serverkey.server_key
    WHERE 1=1
      AND retailer.contract_type IN UNNEST(`kiotvietplus.kv_datawarehouse.contract_type`())              --Lấy gian hàng dùng thật
      AND industry_software = "Retail"                  --Lấy gian hàng dùng giao diện retail
      AND retailer.industry_key IN (9, 11, 15, 27)      -- 13: Nhà thuốc, 32: Thiết bị y tế
      AND DATE(retailer.created_date) <= CURRENT_DATE("UTC+7")-1
      AND DATE(retailer.expiry_date) >= CURRENT_DATE("UTC+7")-1
    )


SELECT DISTINCT k.retailer_id, d.retailer_code, industry_name, SUM(Revenue) revenue, SUM(Margin) margin, SUM(NetProfit) net_profit
FROM `kiotvietplus.kv_datawarehouse.kvretailer_financial_report` k
JOIN re ON k.retailer_id = re.retailer_id
LEFT JOIN `kiotvietplus.kv_crm_dwh.d_retailer_list` d ON k.retailer_id = CAST(d.retailer_id AS STRING)
WHERE DATE(txn_date) >= CURRENT_DATE("UTC+7")-365
GROUP BY 1,2,3
ORDER BY 4 DESC
