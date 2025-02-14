WITH payments AS (
    SELECT * FROM {{ ref('stg_stripe__payments') }}
)
,orders AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__orders') }}
)
,order_payments AS(
    SELECT 
        order_id
        , max(created_at) AS payment_finalized_date
        , SUM(amount) AS total_amount_paid
    FROM payments
    WHERE STATUS <> 'fail'
    GROUP BY 1    
)
,final AS (
    SELECT 
        *
    FROM orders
    LEFT JOIN order_payments using (order_id)
)

SELECT * FROM final