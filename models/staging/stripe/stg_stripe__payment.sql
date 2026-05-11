with 

source as (

    select * from {{ source('stripe', 'payment') }}

),

transformed as (

    select
        id as payment_id,
        orderid as order_id,
        round(amount / 100.0, 2) as payment_amount,
        paymentmethod as payment_method,
        status as payment_status,
        _batched_at,
        created as payment_created_at

    from source

)

select * from transformed