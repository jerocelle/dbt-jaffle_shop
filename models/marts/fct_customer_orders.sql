with

    -- Import CTEs
    customers as (select * from {{ ref('stg_jaffle_shop__customers') }}),

    orders as (select * from {{ ref('stg_jaffle_shop__orders') }}),

    payments as (select * from {{ ref('stg_stripe__payment') }}),

    -- Logical CTEs
    payment_finalized as (
                select
                    order_id,
                    max(payment_created) as payment_finalized_date,
                    payment_amount as total_amount_paid
                from payments
                where payment_status <> 'fail'
                group by 1
            ),

    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.valid_order_date,
            orders.order_status,
            pf.total_amount_paid,
            pf.payment_finalized_date,
            c.givenname as customer_first_name,
            c.surname as customer_last_name
        from orders
        left join payment_finalized pf
            on orders.id = pf.order_id
        left join customers as c on orders.user_id = c.id
    ),

    customer_orders as (
        select
            customers.customer_id,
            min(orders.valid_order_date) as first_order_date,
            max(orders.valid_order_date) as most_recent_order_date,
            count(orders.order_id) as number_of_orders
        from customers as c
        left join orders on orders.user_id = customers.customer_id
        group by 1
    ),

    final as (    
        select
            p.*,
            row_number() over (order by p.order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by p.order_id
            ) as customer_sales_seq,
            case
                when c.first_order_date = p.order_placed_at then 'new' else 'return'
            end as nvsr,
            x.clv_bad as customer_lifetime_value,
            c.first_order_date as fdos
        from paid_orders p
        left join customer_orders as c using (customer_id)
        left outer join
            (
                select p.order_id, sum(t2.total_amount_paid) as clv_bad
                from paid_orders p
                left join
                    paid_orders t2
                    on p.customer_id = t2.customer_id
                    and p.order_id >= t2.order_id
                group by 1
                order by p.order_id
            ) x
            on x.order_id = p.order_id
        order by order_id
    )


    select * from final