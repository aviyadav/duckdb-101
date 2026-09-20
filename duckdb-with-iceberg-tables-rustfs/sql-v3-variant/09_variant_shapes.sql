-- =============================================================================
--  09_variant_shapes.sql
--  VARIANT is self-describing *per row*: the same column can hold objects with
--  different key sets (or entirely different types).
-- =============================================================================

SELECT
    variant_typeof(v) AS variant_shape,
    count(*)          AS events,
    min(id)           AS first_id,
    max(id)           AS last_id
FROM lake.test.gh_shred
GROUP BY 1
ORDER BY events DESC, variant_shape;
