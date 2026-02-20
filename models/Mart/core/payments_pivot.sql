{{ 
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge'
    ) 
}}

with payments as (
    select * from {{ ref('STG_PAYMENTS') }}

    {% if is_incremental() %}
        where loaded_at >= (
            select coalesce(max(loaded_at), to_timestamp('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}
),

final as (
    select
        order_id,
        max(loaded_at) as loaded_at,

        sum(case when lower(payment_method) = 'credit_card' then amount else 0 end) as credit_card,
        sum(case when lower(payment_method) = 'coupon' then amount else 0 end) as coupon,
        sum(case when lower(payment_method) = 'bank_transfer' then amount else 0 end) as bank_transfer,
        sum(case when lower(payment_method) = 'gift_card' then amount else 0 end) as gift_card

    from payments
    group by order_id
)

select * from final
