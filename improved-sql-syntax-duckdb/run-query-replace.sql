-- Exclude ex traditional

SELECT
    transaction_id ,
    timestamp ,
    customer_id ,
    merchant ,
    category ,
    -- multiply amount
    amount*33 AS amount ,
    -- multiply amount
    status
FROM read_parquet('data/*.parquet') LIMIT 5;

-- Exclude ex modern

SELECT * REPLACE(amount*33 AS amount)
FROM read_parquet('data/*.parquet') LIMIT 5;
