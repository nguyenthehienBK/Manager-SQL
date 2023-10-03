MERGE `kiotvietplus-dev.kv_product_suggestion.suggestion_db_v4_test` AS T   -- 50,000
USING `kiotvietplus-dev.kv_product_suggestion.suggestion_db_v5_test` AS S   -- 47,319
ON T._id  = S._id
WHEN NOT MATCHED THEN
  INSERT(
    _id
    ,industry_origin
    ,content
    ,barcode
    ,description
    ,name
    ,industry_new
    ,img
    ,with_barcode
  )
  VALUES(
    S._id
    ,S.industry_origin
    ,S.content
    ,S.barcode
    ,S.description
    ,S.name
    ,S.industry_new
    ,S.img
    ,S.with_barcode
  )
