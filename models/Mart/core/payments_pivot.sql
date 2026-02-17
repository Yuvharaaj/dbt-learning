with payments as (
    select * from {{ ref('STG_PAYMENTS') }}
),

final as (

    select
        order_id,

        {% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

        {% for method in payment_methods %}
            sum(
                case 
                    when payment_method = '{{ method }}'
                    then amount
                    else 0
                end
            ) as {{ method }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from payments
    group by 1
)

select * from final
