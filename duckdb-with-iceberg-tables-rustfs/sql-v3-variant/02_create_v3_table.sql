-- =============================================================================
--  02_create_v3_table.sql
--  Create an Iceberg **format v3** table with a VARIANT column (and a
--  TIMESTAMP_NS column, another v3 data type).
--
--  The table is dropped and re-created so that re-running the exercise is
--  repeatable: `format-version` can only be set at CREATE TABLE time.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS lake.test;

DROP TABLE IF EXISTS lake.test.gh_shred;

CREATE TABLE lake.test.gh_shred (
    id         BIGINT,       -- event id, as in the article
    v          VARIANT,      -- semi-structured GitHub event (the article's column)
    created_at TIMESTAMP_NS  -- v3 data type (article table did not have this)
)
WITH ('format-version' = 3);

-- Show what the catalog stored: the `v` column must be VARIANT,
-- created_at must be TIMESTAMP_NS.
DESCRIBE lake.test.gh_shred;
