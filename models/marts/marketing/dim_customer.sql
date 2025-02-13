WITH customers AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}
)
,fct_orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
)
,customer_orders AS (
    SELECT 
        customer_id
        ,order_id
        ,order_date
        ,order_status
        ,total_amount_paid
        ,payment_finalized_date
        ,ROW_NUMBER()    OVER (PARTITION BY customer_id ORDER BY order_id) as customer_sales_seq
        ,ROW_NUMBER()    OVER (ORDER BY order_id) as transaction_seq
        ,min(order_date) OVER (PARTITION BY customer_id) AS first_order_date
        --,max(order_date) OVER (PARTITION BY customer_id) AS most_recent_order_date
        --,count(order_id) OVER (PARTITION BY customer_id) AS number_of_orders
        ,sum(total_amount_paid) OVER (PARTITION BY customer_id) AS lifetime_value
        ,order_date
     FROM fct_orders
     
)
,final AS (
    SELECT 
        customers.customer_id,
        customer_orders.order_id,
        customer_orders.order_date,
        customer_orders.order_status,
        customer_orders.total_amount_paid,
        customer_orders.payment_finalized_date,
        customers.first_name,
        customers.last_name,
        customer_orders.transaction_seq,
        customer_orders.customer_sales_seq,
        customer_orders.first_order_date,
        CASE WHEN customer_orders.first_order_date = customer_orders.order_date THEN 'new' ELSE 'return' END as nvsr,
        coalesce(customer_orders.lifetime_value,0) AS customer_lifetime_value,
        customer_orders.first_order_date as fdos
    FROM customer_orders
    left join customers using (customer_id)
)

SELECT * FROM final ORDER BY customer_id,order_id 
