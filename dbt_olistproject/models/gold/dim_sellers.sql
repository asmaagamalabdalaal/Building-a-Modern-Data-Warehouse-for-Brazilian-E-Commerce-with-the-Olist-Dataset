{{ config(materialized='view') }}

select
    seller_id,
    seller_zip_code_prefix,
    initcap(trim(seller_city)) as seller_city,       
    upper(trim(seller_state)) as seller_state,
    concat(seller_city,'_',seller_state) as seller_location 
from {{ ref('sellers') }}
where seller_id is not null
