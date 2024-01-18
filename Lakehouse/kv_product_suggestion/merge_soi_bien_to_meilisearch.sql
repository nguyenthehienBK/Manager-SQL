MERGE INTO iceberg.kv_product_suggestion.normalised_product_meilisearch AS T
USING kv_product_suggestion.soi_bien_to_suggestion_meilisearch_2023_12_28 AS S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET
	T.industry_origin = S.industry_origin
	,T.barcode = S.barcode
	,T.industry_name = S.industry_name
    ,T.content = S.content
    ,T.industry_new = S.industry_new
    ,T.description = S.description
    ,T.img = S.img
    ,T.with_barcode = S.with_barcode
    ,T.timestamp = S.timestamp
    ,T.is_valid = TRUE
WHEN NOT MATCHED THEN INSERT
(
	T.id
    ,T.industry_origin
    ,T.content
    ,T.barcode
    ,T.description
    ,T.industry_name
    ,T.industry_new
    ,T.img
    ,T.with_barcode
    ,T.timestamp
    ,T.is_valid
)
VALUES
(
	S.id
    ,S.industry_origin
    ,S.content
    ,S.barcode
    ,S.description
    ,S.industry_name
    ,S.industry_new
    ,S.img
    ,S.with_barcode
    ,S.timestamp
    ,TRUE
)
