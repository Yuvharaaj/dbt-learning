{{ 
    config(
        materialized = 'incremental',
        unique_key = 'customer_id',
        incremental_strategy = 'merge'
    ) 
}}

with customers as (
    select * from {{ ref('STG_CUSTOMER') }}
),

orders as (
    select * from {{ ref('Fact_orders') }}

    {% if is_incremental() %}
        where loaded_at >= (
            select coalesce(max(latest_order_date), to_timestamp('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}
),

customer_orders as (
    select 
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as latest_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as life_time_value
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.latest_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,
        coalesce(co.life_time_value, 0) as life_time_value
    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id
)

select * from final
