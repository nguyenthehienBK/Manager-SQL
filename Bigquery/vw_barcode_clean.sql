##############################################################
# MỤC ĐÍCH: LẤY BARCODE DUY NHẤT CỦA KIOTVIET
# NGƯỜI TẠO VIEW: KHANH.TN
# NGÀY TẠO VIEW: 01/07/2021
# NGƯỜI SỬA: BAO.LD: Bỏ Luhn và ISBN khỏi bộ lọc; Bổ sung 1 số heuristic
# NGƯỜI SỬA: BAO.LD: Bổ sung category_key
# NGƯỜI SỬA: BAO.LD: Bổ sung việc bắt mã vạch của sản phẩm dạng thùng
# NGƯỜI SỬA: hien.nt8@kioviet.com: Bổ sung thêm trường description
##############################################################

with tmp as (select 1)

, kv_barcode as (
select retailer_key as retailer_key
, product.product_key as product_key
, product.real_key as product_id
, RTRIM(
    LTRIM(
      regexp_replace(
        REPLACE(
          REPLACE(product.name, '\n', ' ')
        , '\r',' ')
      , r'\[.*?\+-]', '')
    )
  ) AS product_name
, product.code
, product.barcode                   
, CASE WHEN product.barcode IS NULL THEN regexp_replace(regexp_replace(product.code,'{DEL}',''),'{DEL1}','')           -- nếu 1 product có barcode thì sử dụng giá trị đó
  ELSE regexp_replace(regexp_replace(product.barcode,'{DEL}',''),'{DEL1}','')
  END as barcode_final  
, LOWER(product.unit) as unit
, product.created_date as created_date
, product.category_key
, product.description
FROM `kiotvietplus.kv_datawarehouse.product` product
)

, barcode_int as (
  select retailer_key
  , product_key
  , product_id
  , product_name
  , code
  , barcode
  , regexp_replace(
      regexp_replace(barcode_final, r'\+', '')
    ,'-', '')
  as barcode_clean
  , unit
  , created_date
  , category_key
  , description
  from kv_barcode
  where 1=1
  and SAFE_CAST(barcode_final as INT) is not null
)

, barcode_char as (
  select retailer_key
  , product_key
  , product_id
  , product_name
  , code
  , barcode
  , case 
      # Neu chua - va phan tu dau tien truoc dau - la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '-') and SAFE_CAST(SPLIT(barcode_final, '-')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final,'-')[SAFE_OFFSET(0)] 
      # Neu chua - va phan tu dau tien sau dau - la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '-') and SAFE_CAST(SPLIT(barcode_final, '-')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final,'-')[SAFE_OFFSET(1)] 
      # Neu chua _ va phan tu dau tien truoc dau _ la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '_') and SAFE_CAST(SPLIT(barcode_final, '_')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final,'_')[SAFE_OFFSET(0)] 
      # Neu chua _ va phan tu dau tien sau dau _ la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '_') and SAFE_CAST(SPLIT(barcode_final, '_')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final,'_')[SAFE_OFFSET(1)] 
      # Neu chua + va phan tu dau tien truoc dau + la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'\+') and SAFE_CAST(SPLIT(barcode_final, '+')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final, '+')[SAFE_OFFSET(0)] 
      # Neu chua + va phan tu dau tien sau dau + la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'\+') and SAFE_CAST(SPLIT(barcode_final, '+')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final, '+')[SAFE_OFFSET(1)]
      # Neu chua / va phan tu dau tien truoc dau / la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '/') and SAFE_CAST(SPLIT(barcode_final, '/')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final, '/')[SAFE_OFFSET(0)] 
      # Neu chua / va phan tu dau tien sau dau / la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, '/') and SAFE_CAST(SPLIT(barcode_final, '/')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final, '/')[SAFE_OFFSET(1)]
      # Neu chua \ va phan tu dau tien truoc dau \ la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'\\') and SAFE_CAST(SPLIT(barcode_final, '\\')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final, '\\')[SAFE_OFFSET(0)] 
      # Neu chua \ va phan tu dau tien sau dau \ la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'\\') and SAFE_CAST(SPLIT(barcode_final, '\\')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final, '\\')[SAFE_OFFSET(1)]
      # Neu chua . va phan tu dau tien truoc dau . la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'.') and SAFE_CAST(SPLIT(barcode_final, '.')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final, '.')[SAFE_OFFSET(0)] 
      # Neu chua . va phan tu dau tien sau dau . la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'.') and SAFE_CAST(SPLIT(barcode_final, '.')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final, '.')[SAFE_OFFSET(1)]
      # Neu chua { va phan tu dau tien truoc dau { la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'{') and SAFE_CAST(SPLIT(barcode_final, '{')[SAFE_OFFSET(0)] as INT) is not null then SPLIT(barcode_final, '{')[SAFE_OFFSET(0)] 
      # Neu chua } va phan tu dau tien sau dau } la so INT thi split va lay phan tu do
      when REGEXP_CONTAINS(barcode_final, r'}') and SAFE_CAST(SPLIT(barcode_final, '}')[SAFE_OFFSET(1)] as INT) is not null then SPLIT(barcode_final, '}')[SAFE_OFFSET(1)]
      # Neu ki tu dau tien la chu cai thi xoa ki tu do
      when REGEXP_CONTAINS(left(barcode_final,1), '[a-zA-Z]') then RIGHT(barcode_final, length(barcode_final) - 1)
      # Neu ki tu cuoi cung la chu cai thi xoa ki tu do
      when REGEXP_CONTAINS(right(barcode_final,1), '[a-zA-Z]')then LEFT(barcode_final, length(barcode_final) - 1)
    else barcode_final
    end as barcode_clean
  , unit
  , created_date
  , category_key
  , description
  from kv_barcode
  where 1=1
  and SAFE_CAST(barcode_final as INT) is null
)

, combined as (
  select *
  , CASE
      WHEN (
      (select kiotvietplus.kv_datawarehouse.checkGS1(barcode_int.barcode_clean) = true) 
      -- OR (select kiotvietplus.kv_datawarehouse.checkLuhn(barcode_int.barcode_clean) = true) 
      -- OR (select kiotvietplus.kv_datawarehouse.checkISBN(barcode_int.barcode_clean) = true)
      and safe_cast(barcode_clean AS INT) is not null
      and length (barcode_clean) in (8,10,12,13,14) and not REGEXP_CONTAINS(lower(barcode_clean), r'(^[a-z]|^(00|111|777|1234)|\D|_|\\.)')
      ) THEN 1
      ELSE 0
  END AS valid_barcode
  from barcode_int
  union all 
  select * 
  , CASE
      WHEN (
      (select kiotvietplus.kv_datawarehouse.checkGS1(barcode_char.barcode_clean) = true) 
      -- OR (select kiotvietplus.kv_datawarehouse.checkLuhn(barcode_char.barcode_clean) = true) 
      -- OR (select kiotvietplus.kv_datawarehouse.checkISBN(barcode_char.barcode_clean) = true)
      and safe_cast(barcode_clean AS INT) is not null
      and length (barcode_clean) in (8,10,12,13,14) and not REGEXP_CONTAINS(lower(barcode_clean), r'(^[a-z]|^(0|111|777|1234)|\D|_|\\.)')
      ) THEN 1
      ELSE 0
  END AS valid_barcode
  from barcode_char
)

-- các sản phẩm 14 chữ số, nếu chữ số đầu tiên khác 8, thì là sản phẩm theo dạng multi-item
, combined_multiitem as (
  select 
    * except(barcode_clean),
    case when 
        length(barcode_clean) = 14 
        and SUBSTR(barcode_clean, 1, 1) <> '8' 
        and valid_barcode = 1
      then kiotvietplus.product_discovery.convert_packaging_barcode(barcode_clean) else barcode_clean end as barcode_clean,
    case when 
        length(barcode_clean) = 14 
        and SUBSTR(barcode_clean, 1, 1) <> '8' 
        and valid_barcode = 1
      then barcode_clean end as barcode_clean_package,
    case when 
        length(barcode_clean) = 14 
        and SUBSTR(barcode_clean, 1, 1) <> '8' 
        and valid_barcode = 1
      then 'multi-item' else 'single-item' end as package_size,
  from combined
)

-- các sản phẩm có 12 chữ số thì cần pad số 0 ở đầu
, combined_padded as (
  select
    * except (barcode_clean),
    case
      when length(barcode_clean) = 12 then concat('0', barcode_clean)
      else barcode_clean
    end as barcode_clean
  from combined_multiitem
)

select * from combined_padded
where 1=1
  -- and package_size = 'multi-item'
  -- and valid_barcode = 1
  -- and barcode like '%893482280133%' -- để test
  -- and code like '%893482280133%' -- để test
  -- and date(created_date) >= '2022-10-01' -- để test