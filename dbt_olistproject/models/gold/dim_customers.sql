{{ config(materialized='view') }}
select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from {{ ref('customers') }}
