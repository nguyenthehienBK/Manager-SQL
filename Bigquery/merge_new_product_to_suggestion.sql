MERGE `kiotvietplus-dev.kv_product_suggestion.suggestion_db_v5` AS T
USING `kiotvietplus-dev.kv_product_suggestion.new_product_suggestion_5_7_12_13_27` AS S
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