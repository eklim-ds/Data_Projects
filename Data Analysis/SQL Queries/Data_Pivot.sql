--This query creates pivot table based on Attributes of a product. Attrributes are stored as a nested structure in the [product_table]
with data as  
(
select replace(replace(a.attribute_name, ' ', '_'), '/','_') as name, product_id
from [product_table] p,
Unnest (p.Attributes) as a
where category_name = 'Threaded Rods & Studs'
AND a.attribute_name in ('Thread Size', 'Length', 'Package Quantity', 'Grade/Class', 'Brand', 'Material', 'Threaded Rod Material' ,'Finish', 'Thread Style', 'Item')
),
pivoted_set as (

select 
*
from data
PIVOT
(
    string_agg(attribute_name)
    FOR name in   ('Thread_Size', 'Length', 'Package_Quantity', 'Grade_Class', 'Brand', 'Material',  'Threaded_Rod_Material','Finish', 'Thread_Style', 'Item')
)
)

select product_id,
Thread_Size, 
Length,
Package_Quantity,
Grade_Class,
Brand,
coalesce(Material,Threaded_Rod_Material ) as Material, 
Finish,
Thread_Style,
Item
from pivoted_set

order by product_id
;
