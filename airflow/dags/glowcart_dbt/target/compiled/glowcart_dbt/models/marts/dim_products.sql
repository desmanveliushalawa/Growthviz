with orders as (
    select * from "glowcart_db"."analytics"."stg_orders"
)

select
    product_id,
    product_name,
    count(distinct order_id)        as total_orders,
    sum(quantity)                   as total_quantity_sold,
    sum(total_price)                as total_revenue,
    avg(total_price)                as avg_order_value,
    min(ordered_at)                 as first_order_at,
    max(ordered_at)                 as last_order_at
from orders
group by product_id, product_name
order by total_revenue desc