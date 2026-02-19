    select 
    ID as payment,
    ORDERID as order_id,
    PAYMENTMETHOD as payment_method,
    STATUS,
    {{cent_to_dollar('amount')}} as AMOUNT,
    CREATED as created_at from {{source('JAFFLE_SHOP_RAW_STRIPE','payment')}}