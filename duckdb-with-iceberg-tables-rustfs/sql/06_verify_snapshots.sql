-- =============================================================================
--  06_verify_snapshots.sql
--  Every DuckDB write statement above committed one Iceberg snapshot. The
--  snapshot history lives in PostgreSQL (lakekeeper) -- the data files it
--  points at live in RustFS.
-- =============================================================================

SELECT snapshot_id, sequence_number, timestamp_ms, manifest_list
FROM iceberg_snapshots(lake.lab1.customers2)
ORDER BY sequence_number;
