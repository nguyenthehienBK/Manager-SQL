CREATE OR REPLACE VIEW kv_product_suggestion.vw_soi_bien_to_suggestion_meilisearch (
  _id,
  industry_origin,
  content,
  barcode,
  description,
  industry_name,
  industry_new,
  img,
  with_barcode,
  timestamp,
  is_valid)
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
        --        kv_product_suggestion.bq_product_soibien_clean_manual_with_id) as temp
        kv_product_suggestion.soi_bien_cleaned_2023_12_28) as temp
where
    temp.rn = 1
)
SELECT
    REGEXP_REPLACE(
    	REGEXP_REPLACE(base64(CONCAT(cast(L.name AS string), "-", CAST(9 AS STRING))), r"[\/=]", "_"), r"[\+]", "-"
    ) AS id
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
    ,
    TRUE as is_valid
FROM
    unique_product as L

);