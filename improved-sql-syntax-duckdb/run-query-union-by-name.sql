-- Exclude ex traditional

SELECT
    1 AS source, merchant, category
FROM read_parquet('data/*.parquet')
UNION ALL
SELECT
    2 AS source, category, merchant
FROM read_parquet('data/*.parquet');

-- Exclude ex modern (distinct ON)

SELECT
    1 AS source, merchant, category
FROM read_parquet('data/*.parquet')
UNION ALL BY NAME
SELECT
    2 AS source, category, merchant
FROM read_parquet('data/*.parquet');
