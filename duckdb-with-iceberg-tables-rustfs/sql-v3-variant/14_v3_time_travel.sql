-- =============================================================================
--  14_v3_time_travel.sql
--  Read the table as it was at its first snapshot (before the v3 DELETE), using
--  the catalog table name with a path-style scan function.
-- =============================================================================

SET VARIABLE first_snapshot = (
    SELECT snapshot_id FROM iceberg_snapshots('lake.test.gh_shred')
    ORDER BY sequence_number LIMIT 1
);

SELECT
    getvariable('first_snapshot') AS first_snapshot_id,
    (SELECT count(*) FROM iceberg_scan('lake.test.gh_shred',
                                       snapshot_from_id => getvariable('first_snapshot'))) AS rows_at_first_snapshot,
    (SELECT count(*) FROM lake.test.gh_shred)                                              AS rows_now;
