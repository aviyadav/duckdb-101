-- =============================================================================
--  04_test1_read_one_row.sql        (article: "Test 1: Read a row -> PASS")
--  SELECT * FROM s3_tables_db.test.gh_shred LIMIT 1;
--  The `v` column must come back typed as VARIANT with nested event data.
-- =============================================================================

SELECT * FROM lake.test.gh_shred ORDER BY id LIMIT 1;
