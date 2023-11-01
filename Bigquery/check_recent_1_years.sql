with sale as
  (select distinct product_key from 
    (select distinct 
      product_key
      -- ,max(timestamp) as max_timestam
    from `kiotvietplus.kv_datawarehouse.invoice_detail_facts`
    where date(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    union all
    select distinct 
      product_key
      -- ,max(timestamp) as max_timestam
    from `kiotvietplus.kv_datawarehouse.product`
    where date(created_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))
  )

select distinct industry_key, barcode_clean
from `kiotvietplus.kv_datawarehouse.products_barcode_clean` p join sale on sale.product_key = p.product_key
join `kiotvietplus.kv_datawarehouse.retailer` r on p.retailer_key = r.retailer_key
where p.valid_barcode = 1

-- valid_barcode = 1
-- 1 month: 1,650,903
-- 12 month: 5,410,521

-- valid_barcode = 0
-- 1 month: 7,484,658
-- 12 month: 37,694,212
