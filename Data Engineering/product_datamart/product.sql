/*
This query de-dupes the raw incoming data to produce a final table. 
The query also normalizes data types. 

The process is this:
1. Pick up the newly incoming staged data.
2. Union in the existing data.
3. Prioritize newly incoming staged data over existing data.
4. Prioritize the newest staged data over existing data.
*/
select * except(rn)
from (
  select
    *,
    product_id,
    product_category_name,
    SAFE_CAST(product_name_length as float64),
    SAFE_CAST(product_photos_qty as INT64),
    SAFE_CAST(product_weight_g as float64),
    SAFE_CAST(product_length_cm as float64),
    SAFE_CAST(product_height_cm as float64),
    SAFE_CAST(product_width_cm as float64),
    CURRENT_DATE() as modified_date,
    row_number() over(
      partition by product_id
      order by _partitiondate desc
    ) as rn
  from `product_landing.product` 
  where _partitiondate = @max_partition_date
) a
where a.rn = 1
;