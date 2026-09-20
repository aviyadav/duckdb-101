-- =============================================================================
--  11_variant_any_type.sql
--  A VARIANT value carries its own type: scalars, arrays and objects can live in
--  the same column (same as Snowflake's VARIANT).
-- =============================================================================

SELECT
    variant_typeof(42::VARIANT)             AS int_variant,
    variant_typeof('duckdb'::VARIANT)       AS text_variant,
    variant_typeof([1, 2, 3]::VARIANT)      AS array_variant,
    variant_typeof({'k': 'v'}::VARIANT)     AS object_variant,
    variant_typeof(NULL::VARIANT)           AS null_variant;
