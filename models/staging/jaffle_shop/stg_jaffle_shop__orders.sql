SELECT
    ID as order_id
    , USER_ID as customer_id
    , ORDER_DATE
    , STATUS
FROM {{ source('jaffle_shop', 'orders') }}  