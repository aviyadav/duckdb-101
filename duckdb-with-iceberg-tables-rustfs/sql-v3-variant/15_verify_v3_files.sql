-- =============================================================================
--  15_verify_v3_files.sql
--  Physical layout of the v3 table in RustFS:
--    DATA   / EXISTING         -> Parquet data files
--    DELETE / POSITION_DELETES -> **puffin** binary deletion vectors (v3)
--  File paths are s3://warehouse/iceberg/<table-uuid>/...
-- =============================================================================

SELECT manifest_content, content, file_format, record_count, file_path
FROM iceberg_metadata(lake.test.gh_shred)
ORDER BY manifest_content, content, file_path;
