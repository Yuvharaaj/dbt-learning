select order_id, sum(AMOUNT) as total_amount from {{ ref('STG_PAYMENTS')}} 
group by 1
having not(total_amount >= 0)