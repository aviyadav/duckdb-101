-- =============================================================================
--  17_variant_parquet_shredding.sql
--  Bonus: how the VARIANT column physically lands in RustFS.
--
--  DuckDB shreds VARIANT when writing Parquet (per the Parquet variant spec):
--  the column becomes `metadata` + `value` + `typed_value` groups, with nested
--  `type` / `value` / `typed_value` entries per field (e.g. actor.login). Shredded
--  storage is what makes dot-notation access and predicate pushdown fast.
--  (Amazon S3 Tables "shreds" semi-structured columns the same way - hence the
--   article's table name `gh_shred`.)
-- =============================================================================

SET VARIABLE data_file = (
    SELECT file_path FROM iceberg_metadata(lake.test.gh_shred)
    WHERE manifest_content = 'DATA'
    ORDER BY file_path
    LIMIT 1
);

SELECT getvariable('data_file') AS data_file;

SELECT name, type, repetition_type, num_children
FROM parquet_schema(getvariable('data_file'))
LIMIT 20;
