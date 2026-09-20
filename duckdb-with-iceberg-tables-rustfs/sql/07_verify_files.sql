-- =============================================================================
--  07_verify_files.sql
--  Physical layout of the table:
--    * DATA / EXISTING          -> one Parquet file per INSERT / MERGE branch
--    * DELETE / POSITION_DELETES -> merge-on-read deletes written by MERGE
--  The file paths are s3://warehouse/iceberg/lab1/customers2/... i.e. objects
--  inside the RustFS bucket "warehouse".
-- =============================================================================

SELECT manifest_content, content, file_format, record_count, file_path
FROM iceberg_metadata(lake.lab1.customers2)
ORDER BY manifest_content, file_path;
