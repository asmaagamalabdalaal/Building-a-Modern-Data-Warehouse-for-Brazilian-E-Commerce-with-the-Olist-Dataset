{{ config(materialized='table') }}

with deduped as (select *,row_number() over(partition by customer_id  order by customer_unique_id ) as rn
    from {{ source('BRONZE', 'olist_customers') }}
    where customer_id is not null              
      and customer_unique_id is not null       
      and customer_state is not null           
)
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    initcap(trim(customer_city)) as customer_city,  
    upper(trim(customer_state)) as customer_state   
from deduped
where rn = 1                                      
