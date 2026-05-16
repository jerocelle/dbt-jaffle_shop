{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
    )
}}

with

    customer_orders as (select * from {{ ref("int_customers") }}),


    final as (
        select
            order_id,
            customer_id,
            surname,
            givenname,
            customer_first_order_date as first_order_date,
            customer_order_count as order_count,
            customer_total_lifetime_value as total_lifetime_value,
            order_value_dollars,
            order_status,
            payment_status,

        from customer_orders
    )

select *
from final


{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where first_order_date > (select max(first_order_date) from {{ this }}) 
{% endif %}
