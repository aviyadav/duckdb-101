-- =============================================================================
--  10_variant_helpers.sql
--  The iceberg/VARIANT toolbelt: variant_extract (no dot notation needed) and
--  variant_normalize (canonical binary form), plus what is available in DuckDB:
--    variant_typeof, variant_extract, variant_normalize,
--    variant_bytes_to_variant, variant_to_parquet_variant
-- =============================================================================

SELECT
    id,
    variant_extract(v, 'actor')             AS actor_variant,   -- stays VARIANT
    variant_extract(v, 'actor')::VARCHAR    AS actor_as_text,   -- cast for display
    variant_extract(v, 'repo').name::VARCHAR AS repo_name,
    variant_typeof(variant_normalize(v))    AS normalized_type
FROM lake.test.gh_shred
ORDER BY id
LIMIT 3;
