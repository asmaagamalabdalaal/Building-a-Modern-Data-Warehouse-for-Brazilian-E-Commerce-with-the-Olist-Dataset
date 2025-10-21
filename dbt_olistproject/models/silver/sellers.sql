{{ config(materialized='table') }}

with cleaned as (
    select *
    from {{ source('BRONZE', 'olist_sellers') }}
    where seller_id is not null
)
select
    seller_id,
    seller_zip_code_prefix,
    initcap(trim(seller_city)) as seller_city,  
    upper(trim(seller_state)) as seller_state   
from cleaned
