with orders as (
    select
    id as order_id,
    user_id as customer_id,
    order_date,
    status from {{source('JAFFLE_SHOP_RAW_CO','orders')}}
)

select * from orders