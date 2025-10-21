{{ config(materialized='view') }}

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    datediff(day, o.order_purchase_ts, o.order_approved_ts) as approval_days,  -- Derived Column
    case when i.price > 100 then 'High' else 'Low' end as price_category          -- Derived Column
from {{ ref('orders') }} o
join {{ ref('order_items') }} i
on o.order_id = i.order_id
where o.order_status is not null
