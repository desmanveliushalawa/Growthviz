with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_name,
        customer_email,
        product_id,
        product_name,
        quantity,
        total_price,
        status,
        ordered_at,
        processed_at,
        date(ordered_at) as order_date
    from source
    where order_id is not null
      and total_price > 0
)

select * from cleaned