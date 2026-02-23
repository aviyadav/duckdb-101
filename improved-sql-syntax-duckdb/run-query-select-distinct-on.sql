-- Exclude ex traditional

WITH CTE AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category) as rn
    FROM read_parquet('data/*.parquet')
)
SELECT * EXCLUDE(rn)
FROM CTE
WHERE rn = 1;

-- Exclude ex modern (distinct ON)

SELECT DISTINCT ON (category) *
FROM read_parquet('data/*.parquet');
