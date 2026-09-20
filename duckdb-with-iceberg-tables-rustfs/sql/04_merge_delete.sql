-- =============================================================================
--  04_merge_delete.sql
--  The same MERGE statement can also express a delete set.
-- =============================================================================

MERGE INTO lake.lab1.customers2 AS target
USING (FROM (VALUES (3)) t(customer_id)) AS deletes
ON target.customer_id = deletes.customer_id
WHEN MATCHED THEN DELETE;

SELECT '03-delete' AS step, * FROM lake.lab1.customers2 ORDER BY customer_id;
