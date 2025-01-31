SELECT
    ID
    , ORDERID AS order_id
    , PAYMENTMETHOD
    , STATUS
    , AMOUNT / 100 AS AMOUNT
    , CREATED as CREATED_AT
FROM {{ source('stripe', 'payment') }}