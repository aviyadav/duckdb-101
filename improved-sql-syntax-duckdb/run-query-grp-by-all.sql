-- Exclude ex traditional

SELECT
    DATE_TRUNC('month', timestamp ) AS mth,
    status,
    SUM(amount) AS sum_amount
FROM read_parquet('data/*.parquet')
-- List down all columns
GROUP BY 1, 2;

-- Exclude ex modern (distinct ON)

SELECT
    DATE_TRUNC('month', timestamp ) AS mth,
    status,
    SUM(amount) AS sum_amount
FROM read_parquet('data/*.parquet')
-- Static GROUP BY ALL
GROUP BY ALL;
