{{ config(materialized='table') }}

with cleaned as ( select * from {{ source('BRONZE', 'olist_orders') }}
      where order_id is not null
      and customer_id is not null
      and order_purchase_timestamp is not null
)
select
    order_id,
    customer_id,
    lower(trim(order_status)) as order_status,   
    try_to_timestamp(order_purchase_timestamp) as order_purchase_ts,
    try_to_timestamp(order_approved_at) as order_approved_ts,
    try_to_timestamp(order_delivered_carrier_date) as order_carrier_ts,
    try_to_timestamp(order_delivered_customer_date) as order_delivered_ts,
    try_to_timestamp(order_estimated_delivery_date) as order_estimated_ts,
    datediff(
        day, 
        try_to_timestamp(order_purchase_timestamp), 
        try_to_timestamp(order_delivered_customer_date)
    ) as delivery_days 
from cleaned
