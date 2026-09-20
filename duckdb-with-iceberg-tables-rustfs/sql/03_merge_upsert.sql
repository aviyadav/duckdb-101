-- =============================================================================
--  03_merge_upsert.sql   (the headline example from the article)
--  One statement that updates existing rows and inserts new ones:
--    Alice 90 -> 150 (matched)       Bob  Seattle -> Portland (matched)
--    Dan    inserted (not matched)   Carol untouched
-- =============================================================================

MERGE INTO lake.lab1.customers2 AS target
USING (
    FROM (VALUES
        (1, 'Alice', 'Boston',   150.00),
        (2, 'Bob',   'Portland', 250.50),
        (4, 'Dan',   'Denver',   300.00)
    ) t(customer_id, name, city, balance)
) AS upserts
ON target.customer_id = upserts.customer_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

SELECT '02-upsert' AS step, * FROM lake.lab1.customers2 ORDER BY customer_id;
