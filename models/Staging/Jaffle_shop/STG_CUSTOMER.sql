with customers as (
    select 
    id as customer_id,
    first_name,
    last_name from {{source('JAFFLE_SHOP','customers')}}
)

select * from customers