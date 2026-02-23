-- Exclude ex traditional

SELECT CAST(timestamp AS date) AS dt
FROM read_parquet('data/*.parquet') LIMIT 5;

-- Exclude ex modern

SELECT timestamp::date AS dt
FROM read_parquet('data/*.parquet') LIMIT 5;
