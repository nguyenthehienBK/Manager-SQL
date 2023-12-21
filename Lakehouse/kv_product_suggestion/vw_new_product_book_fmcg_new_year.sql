-- kv_product_suggestion.vw_new_product_book_fmcg_new_year source

CREATE OR REPLACE VIEW kv_product_suggestion.vw_new_product_book_fmcg_new_year (
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
  'transient_lastDdlTime' = '1699357114')
AS (
	with temp_book as (
SELECT
    *
from
    kv_product_suggestion.new_product_non_barcode_full_active_1y
where
    industry_origin = 6
    and (lower(trim(content)) like "%cánh diều%"
        or lower(trim(content)) like "%canh dieu%"
            or lower(trim(content)) like "%sách giáo khoa%"
                or lower(trim(content)) like "%sach giao khoa%")
union
SELECT
    *
from
    kv_product_suggestion.vw_new_product_book
)
,
temp_fmcg as (
SELECT
    *
from
    kv_product_suggestion.new_product_non_barcode_full_active_1y
WHERE
    industry_origin = 9
    and (lower(trim(content)) like "%tết%"
        or lower(trim(content)) like "%tet%"
            or lower(trim(content)) like "%năm%"
                or lower(trim(content)) like "%năm mới%"
                    or lower(trim(content)) like "%nam moi%"
                        or lower(trim(content)) like "%new year%"
                            or lower(trim(content)) like "%noel%"
                                or lower(trim(content)) like "%giáng sinh%"
                                    or lower(trim(content)) like "%giang sinh%"
                                        or lower(trim(content)) like "%cây thông%"
                                            or lower(trim(content)) like "%cay thong%")
UNION
SELECT
    *
from
    kv_product_suggestion.new_product_non_barcode_full_active_1y
WHERE
    industry_origin = 9
    and date(timestamp) BETWEEN DATE("2022-09-01") and DATE("2023-01-01")
)

,
union_book_fmcg as (
SELECT
    *
FROM
    temp_book
UNION
SELECT
    *
FROM
    temp_fmcg
)
select
    *
from
    union_book_fmcg
);