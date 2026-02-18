select order_id
from {{ ref('STG_PAYMENTS') }}
group by order_id
having sum(amount) < 0
