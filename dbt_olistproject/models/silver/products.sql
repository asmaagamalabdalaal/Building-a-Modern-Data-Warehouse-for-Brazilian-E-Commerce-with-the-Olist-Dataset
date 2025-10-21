{{ config(materialized='table') }}

with cleaned as (
    select *
    from {{ source('BRONZE', 'olist_products') }}
    where product_id is not null
)
select
    product_id,
    lower(trim(product_category_name)) as product_category_name, 
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    coalesce(product_weight_g,0)/1000 as product_weight_kg,       
    coalesce(product_length_cm,0) as product_length_cm,
    coalesce(product_height_cm,0) as product_height_cm,
    coalesce(product_width_cm,0) as product_width_cm
from cleaned
