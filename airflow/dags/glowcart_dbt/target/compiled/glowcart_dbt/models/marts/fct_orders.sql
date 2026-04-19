with orders as (
    select * from "glowcart_db"."analytics"."stg_orders"
)

select
    order_id,
    customer_name,
    customer_email,
    product_id,
    product_name,
    quantity,
    total_price,
    status,
    order_date,
    ordered_at,
    processed_at
from orders