-- kv_product_suggestion.vw_new_product_book source

CREATE OR REPLACE VIEW kv_product_suggestion.vw_new_product_book (
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
  'transient_lastDdlTime' = '1699359405')
AS with
raw as (
SELECT
    kv_product.retailer_key
,
    kv_retailer.real_key as retailer_id
,
    kv_industry.v_industry_name as kv_industry
,
    kv_product.product_key
,
    kv_product.barcode_clean as barcode
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
                              REGEXP_REPLACE(UPPER(kv_product.product_name), r'\(.*?\)', '')
                            , r'\[.*?\]', '')
                          , '\n', ' ')
                        , '\r', ' ')
                      , '  ', ' ')
              , r'MẪU MỚI|HCN.|ĐỒNG GIÁ|KHUYẾN MÃI|KHUYẾN MẠI|TẶNG|KÈM|TẶNG KÈM|KHAI TRƯƠNG|GIÁ SỐC', '')-- Loại bỏ một số cụm từ đặc trưng của từng gian hàng
      , r' - ', ' ')-- Thay gạch nối bằng khoảng trắng
    )
  ) AS product_name
,
    kv_industry.industry_key

,
    kv_product.description as description
FROM
    kv_product_suggestion.barcode_clean kv_product
JOIN kvretail_warehouse.retailer_dim as kv_retailer on
    kv_retailer.retailer_key = kv_product.retailer_key
JOIN kvretail_warehouse.industry_dim as kv_industry ON
    kv_industry.industry_key = kv_retailer.industry_key
where
    1 = 1
    and kv_product.valid_barcode = 1
    --     and kv_industry.industry_key in (0, 1, 2, 5, 6, 7, 9, 11, 12, 13, 15, 27)
    and kv_industry.industry_key = 6
    and (LOWER(trim(kv_product.product_name)) LIKE '%canh dieu%'
        OR LOWER(trim(kv_product.product_name)) LIKE '%cánh diều%')
),

kv_img_url as (
-- Lay image url cua cac san pham
SELECT
    product_key,
    real_key,
    image
FROM
    kvretail_warehouse.product_image_dim
)
,
kv_barcode as
(
select
    reflect('com.citigo.udf.from_bq.CleanKvProductName',
    'cleanKvProductName',
    raw.product_name) as name
    -- Clean tên sản phẩm chứa cả giá ("K") và chuỗi ("SALE")
,
    raw.barcode as barcode
,
    img_url.image as images
,
    raw.description as description
,
    CASE
        when industry_key = 27 then 9
        else industry_key
    END as industry
,
    'kiotviet' as barcode_source
,
    raw.kv_industry as industry_name
from
    raw
left join kv_img_url img_url on
    raw.product_key = img_url.product_key
where
    1 = 1

),

kv_barcode_with_name_most_use as
(
SELECT
    *
from
    (
    SELECT
        temp.barcode
        ,
        temp.industry
			,
        temp.name
			,
        temp.cnt
			,
        ROW_NUMBER() OVER (PARTITION BY temp.barcode,
        temp.industry
    ORDER BY
        temp.cnt DESC) rn
    from
        (
        SELECT
            barcode
            ,
            industry
				,
            name
				,
            count(*) as cnt
        from
            kv_barcode
        group by
            barcode,
            industry,
            name) as temp)
where
    rn = 1
),
kv_barcode_final as (
SELECT
    kb.name
		,
    kb.barcode
		,
    kb.images
		,
    kb.description
		,
    kb.industry
		,
    kb.industry_name
		,
    kb.barcode_source
		,

    ROW_NUMBER () OVER (PARTITION BY kb.barcode,
    kb.industry,
    kb.name
ORDER BY
    LENGTH(kb.description) DESC) row_number
from
    kv_barcode as kb
join kv_barcode_with_name_most_use as kbw
		on
    kb.barcode = kbw.barcode
    and kb.industry = kbw.industry
    and kb.name = kbw.name
)

select
    base64((CONCAT(CAST(L.barcode AS STRING), "-", CAST(L.industry AS STRING)))) as _id
  ,
    L.industry AS industry_origin
  ,
    L.name as content
    -- Tên sản phẩm
  ,
    L.barcode AS barcode
  ,
    CASE
        WHEN L.description IS NULL THEN ''
        ELSE L.description
    END AS description
  ,
    L.industry_name
    -- Tên ngành hàng
  ,
    CASE
        WHEN L.industry = 1
        OR L.industry = 2 THEN 101
        WHEN L.industry = 9
        OR L.industry = 11
        OR L.industry = 15 THEN 100
        WHEN L.industry = 12
        OR L.industry = 13 THEN 102
        ELSE L.industry
    END AS industry_new
,
    CASE
        WHEN L.images IS NULL THEN ''
        ELSE L.images
    END AS img
,
    CASE
        WHEN L.barcode = ""
        OR L.barcode IS NULL THEN 0
        ELSE 1
    END AS with_barcode
,
    `current_timestamp`() as timestamp
from
    kv_barcode_final as L
where
    L.row_number = 1;