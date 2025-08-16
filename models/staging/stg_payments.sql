select
    id as payment_id,
    order_id,
    payment_method,
    'success' as status,
    -- amount is stored in cents, convert it to dollars
    {{ cents_to_dollars('amount') }} as amount
from {{ source('stripe', 'payment') }}
