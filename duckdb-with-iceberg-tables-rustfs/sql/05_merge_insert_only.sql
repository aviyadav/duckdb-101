-- =============================================================================
--  05_merge_insert_only.sql
--  Insert-only merge: load new rows without touching existing ones.
-- =============================================================================

MERGE INTO lake.lab1.customers2 AS target
USING (
    FROM (VALUES
        (5, 'Eve',   'Miami',   50.00),
        (6, 'Frank', 'Chicago', 125.00)
    ) t(customer_id, name, city, balance)
) AS new_rows
ON target.customer_id = new_rows.customer_id
WHEN NOT MATCHED THEN INSERT;

-- Re-running the three merge scripts must not duplicate these rows.
SELECT '04-insert-only' AS step, * FROM lake.lab1.customers2 ORDER BY customer_id;
