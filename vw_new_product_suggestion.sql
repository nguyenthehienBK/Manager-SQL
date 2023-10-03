-- ##############################################################################################################################################################
-- Mục đích: Cập nhật dữ liệu New Product để indexing dữ liệu này vào Elasticsearch database suggestion cho KV Retail hàng tuần/ngày
-- Người tạo: hien.nt8@kiotviet.com
-- Tham khảo: kiotvietplus.kv_product_suggestion.vw_barcode_to_suggestion
-- Lần sửa đổi cuối cùng: 02/10/2023
-- Mục đích sửa:Lấy dữ liệu ngành vật liệu xây dụng (5), điện tử điện máy (7), nhà thuốc (13) và siêu thị mini (27) để bổ sung dữ liệu cho bộ suggestion
-- ##############################################################################################################################################################

with raw as (
SELECT kv_product.retailer_key
, kv_retailer.real_key as retailer_id
, kv_industry.v_industry_name as kv_industry
, kv_product.product_key
, kv_product.barcode_clean as barcode
, kv_product.product_name as product_name_original 
, RTRIM(
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
                        , '\r',' ')
                      ,'  ',' ')
              , r'MẪU MỚI|HCN.|ĐỒNG GIÁ|KHUYẾN MÃI|KHUYẾN MẠI|TẶNG|KÈM|TẶNG KÈM|KHAI TRƯƠNG|GIÁ SỐC', '') -- Loại bỏ một số cụm từ đặc trưng của từng gian hàng
      , r' - ', ' ') -- Thay gạch nối bằng khoảng trắng
    )
  ) AS product_name
, kv_industry.industry_key
, kv_product.description
-- FROM `kiotvietplus-dev.kv_datawarehouse.vw_products_barcode_clean_test` kv_product
FROM `kiotvietplus.kv_datawarehouse.products_barcode_clean` kv_product
JOIN `kiotvietplus.kv_datawarehouse.retailer` as kv_retailer on kv_retailer.retailer_key = kv_product.retailer_key
JOIN `kiotvietplus.kv_datawarehouse.industry` as kv_industry ON kv_industry.industry_key = kv_retailer.industry_key

where 1=1
-- DATE(kv_product.timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
and kv_product.valid_barcode =1
and kv_industry.industry_key in (5, 7, 13, 27)
)
,kv_img_url as (-- Lay image url cua cac san pham
  SELECT product_key, real_key, image
    ,ROW_NUMBER() OVER (PARTITION BY product_key ORDER BY real_key DESC) as row_number
  -- FROM `kiotvietplus-dev.kv_datawarehouse.d_product_image_hiennt`
  FROM `kiotvietplus.kv_datawarehouse.d_product_image`

)

, kv_barcode as 
(select 
kiotvietplus.kv_datawarehouse.clean_kv_product_name(raw.product_name) as name # Clean tên sản phẩm chứa cả giá ("K") và chuỗi ("SALE")
, raw.barcode as barcode
, img_url.image as images
, raw.description as description
, industry_key as industry
, 'kiotviet' as barcode_source
, raw.kv_industry as industry_name
from raw 
join kv_img_url img_url on raw.product_key = img_url.product_key
where img_url.image != '0'
group by name, barcode, images, description, industry, industry_name
)

, kv_barcode_final as (
select kv_barcode.name
, kv_barcode.barcode
, kv_barcode.images
, kv_barcode.description
, kv_barcode.industry
, kv_barcode.barcode_source
, kv_barcode.industry_name
, ROW_NUMBER() OVER (PARTITION BY kv_barcode.barcode ORDER BY length(kv_barcode.name) DESC) row_number
from kv_barcode
where length(kv_barcode.barcode) >= 8
)

select
  kiotvietplus.kv_product_suggestion.StrToBase64Str((CONCAT(CAST(L.barcode AS STRING), "-",CAST(L.industry AS STRING)))) as _id
  ,L.industry AS industry_origin
  ,L.name as content # Tên sản phẩm
  ,L.barcode AS barcode
  ,L.description AS description
  ,L.industry_name AS name # Tên ngành hàng
  ,CASE
        WHEN L.industry = 1
        OR L.industry = 2 THEN 101
        WHEN L.industry = 6 THEN 6
        WHEN L.industry = 9
        OR L.industry = 11
        OR L.industry = 15 THEN 100
        ELSE L.industry
    END AS industry_new
  ,L.images as img
  ,CASE
    WHEN L.barcode = ""
    OR L.barcode IS NULL THEN 0
    ELSE 1
  END AS with_barcode
from kv_barcode_final as L
where 1=1
and row_number = 1
