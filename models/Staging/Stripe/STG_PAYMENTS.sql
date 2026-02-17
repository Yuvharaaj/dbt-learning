    select 
    ID as payment,
    ORDERID as order_id,
    PAYMENTMETHOD as payment_method,
    STATUS,
    {{cent_to_dollar('amount')}} as AMOUNT,
    CREATED as created_at from {{source('Stripe','payment')}}