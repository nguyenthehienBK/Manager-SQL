-- kv_product_suggestion.vw_barcode_clean source

CREATE OR REPLACE VIEW kv_product_suggestion.vw_barcode_clean (
  retailer_key,
  product_key,
  product_id,
  product_name,
  code,
  barcode,
  unit,
  created_date,
  category_key,
  description,
  valid_barcode,
  barcode_clean_package,
  package_size,
  barcode_clean)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1703212053')
AS WITH
  kv_barcode AS (
SELECT
    retailer_key AS retailer_key,
    product.product_key AS product_key,
    product.real_key AS product_id,
    RTRIM( LTRIM( REGEXP_REPLACE( REPLACE( REPLACE(product.name, '\n', ' '), '\r', ' '), r'\[.*?\+-]', '') ) ) AS product_name,
    product.code,
    product.barcode,
    CASE
        WHEN product.barcode IS NULL THEN REGEXP_REPLACE(regexp_replace(product.code, '(\\{DEL}+)', ''), '(\\{DEL1}+)', '')
        -- nếu 1 product có barcode thì sử dụng giá trị đó
        ELSE
    REGEXP_REPLACE(regexp_replace(product.barcode,
        '(\\{DEL}+)',
        ''), '(\\{DEL1}+)', '')
    END
    AS barcode_final,
    LOWER(product.unit) AS unit,
    product.created_date AS created_date,
    product.category_key,
    product.description
FROM
    kvretail_warehouse.product_dim product
WHERE
    1 = 1 ),
  barcode_int AS (
SELECT
    retailer_key,
    product_key,
    product_id,
    product_name,
    code,
    barcode,
    REGEXP_REPLACE( REGEXP_REPLACE(barcode_final, r'\+', ''), '-', '') AS barcode_clean,
    unit,
    created_date,
    category_key,
    description
FROM
    kv_barcode
WHERE
    1 = 1
    AND CAST(barcode_final AS BIGINT) IS NOT NULL ),
  barcode_char AS (
SELECT
    retailer_key,
    product_key,
    product_id,
    product_name,
    code,
    barcode,
    CASE
        -- Neu chua - va phan tu dau tien truoc dau - la so INT thi split va lay phan tu do
      WHEN REGEXP_LIKE(barcode_final,
        '[-]')
            AND CAST (SUBSTRING(SPLIT_PART(barcode_final, '-', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '-', 1),-1)
            -- Neu chua - va phan tu dau tien sau dau - la so INT thi split va lay phan tu do
            WHEN REGEXP_LIKE(barcode_final,
            '[-]')
                AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
          '-',
          2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
        '-',
        2), 1, 1)
                -- Neu chua _ va phan tu dau tien truoc dau _ la so INT thi split va lay phan tu do
                WHEN REGEXP_LIKE(barcode_final,
                '[_]')
                    AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '_', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '_', 1),-1)
                    -- Neu chua _ va phan tu dau tien sau dau _ la so INT thi split va lay phan tu do
                    WHEN REGEXP_LIKE(barcode_final,
                    '[_]')
                        AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
          '_',
          2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
        '_',
        2), 1, 1)
                        -- Neu chua + va phan tu dau tien truoc dau + la so INT thi split va lay phan tu do
                        WHEN REGEXP_LIKE(barcode_final,
                        r'[\+]')
                            AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '+', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '+', 1),-1)
                            -- Neu chua + va phan tu dau tien sau dau + la so INT thi split va lay phan tu do
                            WHEN REGEXP_LIKE(barcode_final,
                            r'[\+]')
                                AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
          '+',
          2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
        '+',
        2), 1, 1)
                                -- Neu chua / va phan tu dau tien truoc dau / la so INT thi split va lay phan tu do
                                WHEN REGEXP_LIKE(barcode_final,
                                '[/]')
                                    AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '/', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '/', 1),-1)
                                    -- Neu chua / va phan tu dau tien sau dau / la so INT thi split va lay phan tu do
                                    WHEN REGEXP_LIKE(barcode_final,
                                    '[/]')
                                        AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
          '/',
          2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
        '/',
        2), 1, 1)
                                        -- Neu chua \ va phan tu dau tien truoc dau \ la so INT thi split va lay phan tu do
                                        WHEN REGEXP_LIKE(barcode_final,
                                        r'[\\]')
                                            AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '\\', 1),-1) as bigint) is not null then substring(SPLIT_PART(barcode_final, '\\', 1),-1)
                                            -- Neu chua \ va phan tu dau tien sau dau \ la so INT thi split va lay phan tu do
                                            WHEN REGEXP_LIKE(barcode_final,
                                            r'[\\]')
                                                AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
                '\\', 2), 1, 1) as bigint) is not null then substring(SPLIT_PART(barcode_final, '\\', 2), 1, 1)
                                                -- Neu chua . va phan tu dau tien truoc dau . la so INT thi split va lay phan tu do
                                                WHEN REGEXP_LIKE(barcode_final,
                                                r'[.]')
                                                    AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '.', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '.', 1),-1)
                                                    -- Neu chua . va phan tu dau tien sau dau . la so INT thi split va lay phan tu do
                                                    WHEN REGEXP_LIKE(barcode_final,
                                                    r'[.]')
                                                        AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
                      '.',
                      2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
                    '.',
                    2), 1, 1)
                                                        -- Neu chua { va phan tu dau tien truoc dau { la so INT thi split va lay phan tu do
                                                        WHEN REGEXP_LIKE(barcode_final,
                                                        r'[{]')
                                                            AND CAST(SUBSTRING(SPLIT_PART(barcode_final, '{', 1),-1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final, '{', 1),-1)
                                                            -- Neu chua } va phan tu dau tien sau dau } la so INT thi split va lay phan tu do
                                                            WHEN REGEXP_LIKE(barcode_final,
                                                            r'[}]')
                                                                AND CAST(SUBSTRING(SPLIT_PART(barcode_final,
                      '}',
                      2), 1, 1) AS bigint) IS NOT NULL THEN SUBSTRING(SPLIT_PART(barcode_final,
                    '}',
                    2), 1, 1)
                                                                -- Neu ki tu dau tien la chu cai thi xoa ki tu do
                                                                WHEN REGEXP_LIKE(SUBSTRING(barcode_final, 1, 1),
                                                                '[a-zA-Z]') THEN SUBSTRING(barcode_final, 2)
                                                                -- Neu ki tu cuoi cung la chu cai thi xoa ki tu do
                                                                WHEN REGEXP_LIKE(SUBSTRING(barcode_final,-1),
                                                                '[a-zA-Z]')THEN SUBSTRING(barcode_final, 1, LENGTH(barcode_final)-1)
                                                                ELSE
                barcode_final
                                                            END
                AS barcode_clean,
                                                            unit,
                                                            created_date,
                                                            category_key,
                                                            description
                                                        FROM
                                                            kv_barcode
                                                        WHERE
                                                            1 = 1
                                                            AND CAST(barcode_final AS BIGINT) IS NULL ),
combined AS (
SELECT
                *,
                CASE
                  WHEN ( (
        SELECT
            reflect('com.citigo.udf.kv_datawarehouse.CheckGS1',
            'checkGS1',
            barcode_char.barcode_clean) = TRUE)
            AND CAST(barcode_clean AS BIGINT) IS NOT NULL
                AND length (barcode_clean) IN (8, 10, 12, 13, 14)
                    AND NOT REGEXP_LIKE(LOWER(barcode_clean),
                    r'(^[a-z]|^(00|111|777|1234)|\D|_|\\.)') ) THEN 1
        ELSE
                0
    END
                AS valid_barcode
FROM
                barcode_char
UNION ALL
SELECT
                *,
                CASE
                  WHEN ( (
        SELECT
            reflect('com.citigo.udf.kv_datawarehouse.CheckGS1',
            'checkGS1',
            barcode_int.barcode_clean) = TRUE)
            AND CAST(barcode_clean AS BIGINT) IS NOT NULL
                AND length (barcode_clean) IN (8, 10, 12, 13, 14)
                    AND NOT REGEXP_LIKE(LOWER(barcode_clean),
                    r'(^[a-z]|^(0|111|777|1234)|\D|_|\\.)') ) THEN 1
        ELSE
                0
    END
                AS valid_barcode
FROM
                barcode_int ),
-- các sản phẩm 14 chữ số, nếu chữ số đầu tiên khác 8, thì là sản phẩm theo dạng multi-item
combined_multiitem AS (
SELECT
                `(barcode_clean)?+.+`
                ,
                CASE
                  WHEN LENGTH(barcode_clean) = 14
            AND SUBSTR(barcode_clean, 1, 1) <> '8'
                AND valid_barcode = 1 THEN (
                SELECT
                    (CONCAT(SUBSTR(barcode_clean, 2, LENGTH(barcode_clean)-2), reflect('com.citigo.udf.kv_datawarehouse.CalculateGS1CheckDigit', 'calculateGS1CheckDigit', SUBSTR(barcode_clean, 2, LENGTH(barcode_clean)-2) ) )))
                ELSE
                barcode_clean
            END
                AS barcode_clean
                ,
                CASE
                  WHEN LENGTH(barcode_clean) = 14
                    AND SUBSTR(barcode_clean, 1, 1) <> '8'
                        AND valid_barcode = 1 THEN barcode_clean
                    END
                AS barcode_clean_package,
                    CASE
                        WHEN LENGTH(barcode_clean) = 14
                            AND SUBSTR(barcode_clean, 1, 1) <> '8'
                                AND valid_barcode = 1 THEN 'multi-item'
                                ELSE
                'single-item'
                            END
                AS package_size
                        FROM
                            combined ),
-- các sản phẩm có 12 chữ số thì cần pad số 0 ở đầu                            
combined_padded AS (
SELECT
                `(barcode_clean)?+.+`
                ,
                CASE
                  WHEN LENGTH(barcode_clean) = 12 THEN CONCAT('0', barcode_clean)
        ELSE
                barcode_clean
    END
                AS barcode_clean
FROM
                combined_multiitem )
            SELECT
              *
FROM
              combined_padded
WHERE
              1 = 1;