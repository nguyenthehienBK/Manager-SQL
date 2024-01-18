# MỤC ĐÍCH LẤY CÁC EVENT WEB TRÊN SỬ DỤNG PRODUCT SUGGESTION
# NGƯỜI TẠO: TRANGPTT
# NGÀY TẠO: 18/09/2023

  SELECT DISTINCT
  DATETIME(timestamp,'Asia/Ho_Chi_Minh') AS timestamp
  , "Web" AS platform
  , CAST(retailer_id AS INT64) AS retailer_id
  , SAFE_CAST(user_id AS INT64) AS user_id
  , CASE WHEN event_category = 'product' THEN 'AddProduct' ELSE module END module
  , use_auto_suggest_product
  , event_category AS event_name
  , event_action
  , document_location
  , kv_session
  , session_uuid
  , event_label
  , content_action
  , search_from
  , is_bar_scanner
  , user_query
  , user_select
  , CASE WHEN suggestion_barcode <> '' AND suggestion_barcode IS NOT NULL AND length(suggestion_barcode) >= 8 AND suggestion_barcode = REGEXP_EXTRACT(suggestion_barcode, r'([0-9]+)') THEN 'barcode' ELSE 'non-barcode' END AS product_type  --sản phẩm sau khi được lưu có thông tin barcode hay không
  , suggestion_id
  , suggestion_barcode
  , CAST(suggestion_product_id AS STRING) AS suggestion_product_id
  , suggestion_content 
  , COUNT(*) as count
  FROM `kiotvietplus.kva_datawarehouse.f_kva_click_tracker` c
  WHERE DATE(timestamp,'Asia/Ho_Chi_Minh') = CURRENT_DATE("UTC+7")-1 
  AND event_category IN ('product_suggestion', 'product')
  AND retailer_id IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22