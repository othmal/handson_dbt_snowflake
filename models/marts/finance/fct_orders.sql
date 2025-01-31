WITH payments AS (
    SELECT * FROM {{ ref('stg_stripe__payments') }}
)
,orders AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__orders') }}
)
,order_payments AS(
    SELECT 
        order_id,
        SUM(CASE WHEN STATUS='success' THEN amount END) AS amount
    FROM payments
    GROUP BY 1    
)
,final AS (
    SELECT 
        orders.customer_id
        , orders.order_id
        , orders.order_date
        , order_payments.AMOUNT AS amount
    FROM orders
    LEFT JOIN order_payments using (order_id)
)

SELECT * FROM final