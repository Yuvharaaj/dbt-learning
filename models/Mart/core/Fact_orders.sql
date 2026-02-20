{{ 
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge'
    ) 
}}

with orders as (
    select * from {{ ref('STG_ORDERS') }}

    {% if is_incremental() %}
        where loaded_at >= (
            select coalesce(max(loaded_at), to_timestamp('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}
),

payments as (
    select * from {{ ref('STG_PAYMENTS') }}

    {% if is_incremental() %}
        where loaded_at >= (
            select coalesce(max(loaded_at), to_timestamp('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}
),

order_payments as (
    select 
        order_id,
        sum(case when status = 'success' then amount else 0 end) as amount
    from payments
    group by order_id
),

final as (
    select 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.loaded_at,
        coalesce(op.amount, 0) as amount
    from orders o
    left join order_payments op
        on o.order_id = op.order_id
)

select * from final
