SELECT
    ID
    , ORDERID AS order_id
    , PAYMENTMETHOD
    , STATUS
    , {{ cent_to_dollars('AMOUNT',3) }} AS AMOUNT
    , CREATED as CREATED_AT
FROM {{ source('stripe', 'payment') }}