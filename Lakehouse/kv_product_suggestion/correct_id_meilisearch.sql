-- 20231219
CREATE TABLE iceberg.kv_product_suggestion.normalised_product_meilisearch
USING iceberg
LOCATION 'hdfs://dc2p-bi-namenode-01:9000/kv_product_suggestion_lakehouse/mart/normalised_product_meilisearch'
TBLPROPERTIES (
  'external.table.purge' = 'true',
  'format' = 'iceberg/parquet',
  'format-version' = '1',
  'write.format.default' = 'parquet',
  'write.metadata.previous-versions-max' = '2',
  'write.parquet.compression-codec' = 'snappy')
AS (
	SELECT 
        REGEXP_REPLACE(REGEXP_REPLACE(`_id`, r"[\/=]", "_"), r"[\+]", "-") AS id
		,industry_origin 
		,content 
		,barcode 
		,description 
		,industry_name 
		,industry_new 
		,img 
		,with_barcode
		,`timestamp`
	FROM kv_product_suggestion.normalised_product np
	where LENGTH(content) > 4
);