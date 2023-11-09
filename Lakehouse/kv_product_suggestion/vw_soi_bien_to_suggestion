-- kv_product_suggestion.vw_soi_bien_to_suggestion source

CREATE OR REPLACE VIEW kv_product_suggestion.vw_soi_bien_to_suggestion (
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
  'transient_lastDdlTime' = '1699434803')
AS (
with unique_product as (
SELECT
    *
from
    (
    SELECT
        name
		,
        description
		,
        img
		,
        ROW_NUMBER () OVER (PARTITION BY name
    ORDER BY
        LENGTH(img) DESC) rn
    from
        kv_product_suggestion.bq_product_soibien_clean_manual_with_id) as temp
where
    temp.rn = 1
)
SELECT
    base64(CONCAT(cast(L.name AS string), "-", CAST(9 AS STRING))) AS _id
	,
    9 AS industry_origin
	,
    name AS content
	,
    "" AS barcode
	,
    CASE
        WHEN description IS NULL
			THEN ''
        ELSE description
    END AS description
	,
    "Tạp hóa" as industry_name
	,
    100 AS industry_new
	,
    CASE
        WHEN L.img IS NULL THEN ''
        ELSE L.img
    END AS img
	,
    0 AS with_barcode
	,
    Date(CURRENT_date()) as timestamp
FROM
    unique_product as L

);