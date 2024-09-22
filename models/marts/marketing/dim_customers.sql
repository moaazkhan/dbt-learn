
WITH customers as (

SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}

),

orders as (
SELECT * FROM {{ ref('stg_jaffle_shop__orders') }}
),


customer_orders as (
    SELECT 
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1
),


final as (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.most_recent_order_date,
        ZEROIFNULL(co.number_of_orders) as number_of_orders

    from customers c

    left join customer_orders co
    ON c.customer_id = co.customer_id

)

SELECT * FROM final