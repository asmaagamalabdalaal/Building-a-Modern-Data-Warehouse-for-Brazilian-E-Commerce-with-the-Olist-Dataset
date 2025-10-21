{{ config(materialized='view') }}

with orders_clean as (
    select
        ORDER_PURCHASE_TS::date as order_date,
        PRICE,
        CUSTOMER_ID,
        PRODUCT_ID,
        SELLER_ID,
        FREIGHT_VALUE,
        ORDER_STATUS
    from {{ ref('fact_orders') }}
    where ORDER_STATUS = 'delivered'
)

select
    order_date,
    count(distinct CUSTOMER_ID) as number_of_customers,
    count(*) as number_of_orders,
    sum(PRICE) as total_sales,
    sum(FREIGHT_VALUE) as total_freight
from orders_clean
group by order_date
order by order_date
