{{ config(materialized='view') }}

select
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_kg,      
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_weight_kg / nullif(product_length_cm * product_height_cm * product_width_cm,0) as density_kg_per_cm3  
from {{ ref('products') }}
where product_weight_kg > 0
