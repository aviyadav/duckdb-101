-- =============================================================================
--  12_v3_deletion_vectors.sql
--  Iceberg v3 semantics: DELETEs are stored as *binary deletion vectors*
--  (Puffin files) instead of the positional-delete Parquet files that v2 tables
--  use - compare with exercise 1 (sql/07_verify_files.sql), where the same
--  operation produced `-deletes.parquet` files.
-- =============================================================================

-- Delete one event (id = 8, the DeleteEvent).
DELETE FROM lake.test.gh_shred WHERE id = 8;

SELECT
    (SELECT count(*) FROM lake.test.gh_shred)                          AS rows_after_delete,
    (SELECT count(*) FROM lake.test.gh_shred WHERE id = 8)              AS deleted_id_visible,
    (SELECT file_format FROM iceberg_metadata(lake.test.gh_shred)
       WHERE content = 'POSITION_DELETES' LIMIT 1)                      AS delete_file_format;
