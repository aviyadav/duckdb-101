-- Exclude ex traditional

WITH CTE AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
                          PARTITION BY DATE_TRUNC('month', timestamp )
                          ORDER BY timestamp
                          ) AS rn
    FROM read_parquet('data/*.parquet')
)
SELECT * EXCLUDE(rn)
FROM CTE
WHERE rn = 1;

-- Exclude ex modern (distinct ON)

SELECT
    *
FROM read_parquet('data/*.parquet')
QUALIFY ROW_NUMBER() OVER(
                          PARTITION BY DATE_TRUNC('month', timestamp )
                          ORDER BY timestamp
                          ) = 1;
