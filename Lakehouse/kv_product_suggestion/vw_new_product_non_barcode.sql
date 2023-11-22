-- kv_product_suggestion.vw_new_product_non_barcode source

CREATE OR REPLACE VIEW kv_product_suggestion.vw_new_product_non_barcode (
  _id,
  industry_origin,
  content,
  barcode,
  description,
  industry_name,
  industry_new,
  img,
  with_barcode,
  timestamp)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1700642182')
AS (
with active_1y as (
select
    product_key,
    timestamp
from
    (
    select
        product_key
    ,
        timestamp
    ,
        ROW_NUMBER() OVER (PARTITION BY product_key
    ORDER BY
        timestamp DESC) rn
    from
        (
        select
            product_key
      ,
            date(timestamp) as timestamp
        from
            kvretail_warehouse.invoice_detail_fact
        where
            timestamp >= DATE_SUB(CURRENT_DATE(),
            365)
    union all
        select
            product_key
      ,
            date(created_date) as timestamp
        from
            kvretail_warehouse.product_dim
        where
            date(created_date) >= DATE_SUB(CURRENT_DATE(),
            365)
    union all
        SELECT
            product_key
     		,
            max(created_date) as timestamp
        from
            kvretail_warehouse.purchase_order_detail_fact
            -- Lấy thêm sản phẩm được nhập từ supplier
        where
            created_date >= DATE_SUB(CURRENT_DATE(),
            455)
        group by
            product_key
     	)
) as temp_union
where
    temp_union.rn = 1
)

,
non_barcode_1y as (
SELECT
    kv_product.retailer_key
,
    kv_retailer.real_key as retailer_id
,
    kv_industry.v_industry_name as kv_industry
,
    kv_product.product_key
    --    kv_product.barcode_clean as barcode
,
    kv_product.product_name as product_name_original
,
    RTRIM(
    LTRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
                REPLACE(
                        REPLACE(
                          REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(kv_product.product_name, r'\(.*?\)', '')
                            , r'\[.*?\]', '')
                          , '\n', ' ')
                        , '\r', ' ')
                      , '  ', ' ')
              , r'(?i)mẫu mới|hcn.|đồng giá|khuyến mại|khuyến mãi|tặng|kèm|tặng kèm|khai trương|giá sốc', '')-- Loại bỏ một số cụm từ đặc trưng của từng gian hàng
      , r' - ', ' ')-- Thay gạch nối bằng khoảng trắng
    )
  ) AS product_name
,
    --    kv_industry.industry_key
    CASE
        when kv_industry.industry_key = 27 then 9
        else kv_industry.industry_key
    END as industry_key
,
    active_1y.timestamp
,
    kv_product.description as description
FROM
    kv_product_suggestion.barcode_clean kv_product
JOIN active_1y on
    active_1y.product_key = kv_product.product_key
    -- được tạo hoặc có giao dịch trong vòng 1 năm trở lại
JOIN kvretail_warehouse.retailer_dim as kv_retailer on
    kv_retailer.retailer_key = kv_product.retailer_key
JOIN kvretail_warehouse.industry_dim as kv_industry ON
    kv_industry.industry_key = kv_retailer.industry_key
where
    1 = 1
    and kv_product.valid_barcode = 0
    and kv_product.barcode is null
    and kv_industry.industry_key in (0, 1, 2, 5, 6, 7, 9, 11, 12, 13, 15, 27)
)

, kv_img_url as (
-- Lay image url cua cac san pham
SELECT
    product_key,
    real_key,
    image
FROM
    kvretail_warehouse.product_image_dim
)

,
aggreate_with_img as (
SELECT
    reflect('com.citigo.udf.from_bq.CleanKvProductName',
    'cleanKvProductName',
    nb1y.product_name) as content
    -- Clean tên sản phẩm chứa cả giá ("K") và chuỗi ("SALE")
		,
    nb1y.description
		,
    ki.image as img
    ,
    industry_key
    ,
    nb1y.kv_industry as industry_name
    ,
    nb1y.timestamp
from
    non_barcode_1y nb1y
join kv_img_url ki on
    nb1y.product_key = ki.product_key
WHERE
    length(ki.image) > 25
),

process_product_name as (
SELECT
--    UPPER(REGEXP_REPLACE(lower(content), "mão", "thìn")) as content
	content
		,
    description
		,
    img
    ,
    industry_key
    ,
    industry_name
		,
    timestamp
from
    aggreate_with_img
),

unique_product_name as (
SELECT
    content
		,
    description
		,
    img
    ,
    industry_key
    ,
    industry_name
		,
    timestamp
		,
    ROW_NUMBER () OVER (PARTITION BY UPPER(content),
    industry_key
ORDER BY
    LENGTH(description) DESC) rn
from
    process_product_name
)
SELECT
    base64(CONCAT(cast(UPPER(content) AS string), "-", CAST(L.industry_key AS STRING))) AS _id
	,
    L.industry_key AS industry_origin
	,
    content AS content
	,
    "" AS barcode
	,
    CASE
        WHEN description IS NULL
			THEN ''
        ELSE description
    END AS description
	,
    L.industry_name
	,
    CASE
        WHEN L.industry_key = 1
        OR L.industry_key = 2 THEN 101
        WHEN L.industry_key = 9
        OR L.industry_key = 11
        OR L.industry_key = 15 THEN 100
        WHEN L.industry_key = 12
        OR L.industry_key = 13 THEN 102
        ELSE L.industry_key
    END AS industry_new
	,
    CASE
        WHEN L.img IS NULL THEN ''
        ELSE L.img
    END AS img
	,
    0 AS with_barcode
	,
    L.timestamp
FROM
    unique_product_name as L
WHERE
    rn = 1);