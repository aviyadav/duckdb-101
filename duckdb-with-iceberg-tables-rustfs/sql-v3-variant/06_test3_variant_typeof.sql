-- =============================================================================
--  06_test3_variant_typeof.sql      (article: "Test 3: Variant introspection -> PASS")
--  SELECT variant_typeof(v) FROM s3_tables_db.test.gh_shred LIMIT 5;
-- =============================================================================

SELECT id, variant_typeof(v) AS variant_type
FROM lake.test.gh_shred
ORDER BY id
LIMIT 5;
