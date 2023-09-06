/*
This query extracts package quantity value that is embedded in the title of a product. Some product titles may contain spelling mistakes or invalid package quanity.
(Examples: "School crayons 12pk", "School crayons package 12", "School's crayons pkg of 11")
Package quantity can appear before or after words 'pk', 'pkg' or 'package'. These words can appear anywhere in the beginning, middle or the end of the title.
After that, the query compares quantity value extracted from the title to the package quantity listed for that product.
The query returns all the products where the two values do not match.
*/

WITH default_title_set AS (
SELECT product_id, title,
--clean product title to remove any special characters. Separate all words in the title with " "
TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(title), r"[^a-z0-9]+"," "), r"(\d)([a-z])", r"\1 \2"), r"([a-z])(\d)", r"\1 \2")) as cleaned_title,
--convert title into an array
SPLIT(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(title), r"[^a-z0-9]+"," "), r"(\d)([a-z])", r"\1 \2"), r"([a-z])(\d)", r"\1 \2")),' ') as array_cleaned_title,
CAST(package_quantity as STRING) as package_quantity

FROM [product_table_name_here]
WHERE (LOWER(title) like '%pk%' or LOWER(title) like '%pkg%' or LOWER(title) like '%package%')
)
,
--calculate the positon of words 'pk', 'pkg' or 'package' in product title
pk_position as (
SELECT product_id, index  FROM  default_title_set, UNNEST(array_cleaned_title) as x WITH OFFSET as index WHERE LOWER(x) in ( 'pk','pkg','package')
)

SELECT 
d.product_id,
title,
cleaned_title,
package_quantity,
index,
--return values that surround words 'pk', 'pkg' or 'package'; these values represent product quantities
array_cleaned_title [SAFE_OFFSET(index-1)] as value_before_index,
array_cleaned_title [SAFE_OFFSET(index+1)] as value_after_index

FROM default_set d INNER JOIN pk_position p on d.product_id = p.product_id

WHERE 
--eliminate all values where embedded package quanity is not numeric
(safe_cast(array_cleaned_title [SAFE_OFFSET(index-1)] as Float64) is not null  or safe_cast(array_cleaned_title [SAFE_OFFSET(index+1)] as Float64) is not null)
--return only those products where package quanity that is listed does not match to package quanity in the title
and (package_quantity!=array_cleaned_title [SAFE_OFFSET(index-1)]  and package_quantity!=array_cleaned_title [SAFE_OFFSET(index+1)] )
;
