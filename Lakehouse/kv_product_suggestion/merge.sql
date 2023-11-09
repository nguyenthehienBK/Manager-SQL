
MERGE INTO iceberg.kv_product_suggestion.new_product AS T
USING kv_product_suggestion.new_product_non_barcode AS S
ON T._id = S._id
WHEN NOT MATCHED THEN INSERT
(
	T._id
    ,T.industry_origin
    ,T.content
    ,T.barcode
    ,T.description
    ,T.industry_name
    ,T.industry_new
    ,T.img
    ,T.with_barcode
    ,T.timestamp
)
VALUES
(
	S._id
    ,S.industry_origin
    ,S.content
    ,S.barcode
    ,S.description
    ,S.name
    ,S.industry_new
    ,S.img
    ,S.with_barcode
    ,S.timestamp
);
