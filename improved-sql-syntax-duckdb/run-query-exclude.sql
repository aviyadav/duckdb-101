-- Exclude ex traditional

SELECT
    transaction_id ,
    timestamp ,
    -- customer_id,
    merchant ,
    category ,
    amount ,
    status
FROM read_parquet('data/*.parquet') LIMIT 5;

-- Exclude ex modern

SELECT * EXCLUDE(customer_id)
FROM read_parquet('data/*.parquet') LIMIT 5;
