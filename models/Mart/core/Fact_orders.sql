{{config(materialized = "table")}}
with orders as (
    select * from {{ ref('STG_ORDERS')}} )
    ,
    payments as (
        select * from {{ ref('STG_PAYMENTS')}} )
    ,
    order_payments as (
        select order_id,
        sum(case when status='success' then AMOUNT end) as AMOUNT
        from payments
        group by 1 
    ),
    final as (
        select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce( order_payments.AMOUNT,0) as AMOUNT
        from orders left join order_payments on orders.order_id = order_payments.order_id
    )

    select * from final