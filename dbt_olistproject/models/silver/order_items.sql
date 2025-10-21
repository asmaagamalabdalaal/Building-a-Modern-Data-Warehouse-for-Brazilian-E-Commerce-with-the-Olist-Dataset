{{ config(materialized='table') }}

with cleaned as (
    select *
    from {{ source('BRONZE', 'olist_order_items') }}
    where order_id is not null
      and product_id is not null
      and seller_id is not null
)
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    try_to_timestamp(shipping_limit_date) as shipping_limit_ts,
    price,
    freight_value,
    (price + freight_value) as total_value  
from cleaned
