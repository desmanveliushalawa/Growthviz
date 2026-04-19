with orders as (
    select * from "glowcart_db"."analytics"."stg_orders"
)

select
    order_date,
    count(distinct order_id)                                    as total_orders,
    sum(total_price)                                            as total_revenue,
    avg(total_price)                                            as avg_order_value,
    sum(case when status = 'confirmed' then total_price end)    as confirmed_revenue,
    sum(case when status = 'shipped' then total_price end)      as shipped_revenue,
    sum(case when status = 'pending' then total_price end)      as pending_revenue,
    count(distinct customer_email)                              as unique_customers
from orders
group by order_date
order by order_date desc