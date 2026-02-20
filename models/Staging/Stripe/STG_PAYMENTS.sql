select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    {{ cent_to_dollar('amount') }} as amount,
    created as created_at,
    _batched_at as loaded_at   -- ⭐ REQUIRED FOR INCREMENTAL
from {{ source('JAFFLE_SHOP_RAW_STRIPE','payment') }}
