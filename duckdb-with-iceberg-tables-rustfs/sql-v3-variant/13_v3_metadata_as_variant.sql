-- =============================================================================
--  13_v3_metadata_as_variant.sql
--  Bonus: the catalog's raw LoadTable response exposes the Iceberg table
--  metadata itself as a VARIANT, so metadata fields can be queried with the same
--  dot notation / subscript syntax.
-- =============================================================================

SELECT
    metadata."format-version"      AS format_version,        -- must be 3
    metadata."current-snapshot-id" AS current_snapshot_id,
    metadata."last-sequence-number" AS last_sequence_number,
    metadata.location              AS location,
    variant_typeof(metadata.schemas) AS schemas_variant_type
FROM iceberg_load_table_response(lake.test.gh_shred);
