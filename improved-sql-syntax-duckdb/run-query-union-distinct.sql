-- Exclude ex traditional

WITH CTE AS (
    SELECT
        customer_id
    FROM read_parquet('data/*.parquet')
    UNION ALL
    SELECT
        customer_id
    FROM read_parquet('data/*.parquet')
)
SELECT DISTINCT *
FROM CTE;

-- Exclude ex modern (distinct ON)

SELECT
    customer_id
FROM read_parquet('data/*.parquet')
UNION DISTINCT
SELECT
    customer_id
FROM read_parquet('data/*.parquet');
