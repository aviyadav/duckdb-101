-- =============================================================================
--  01_secrets_and_attach.sql   (exercise 2: Iceberg format v3 + VARIANT)
--
--  Local version of:
--    "Can DuckDB Read Iceberg V3 VARIANT from Amazon S3 Tables? Quick Test"
--    https://medium.com/@shahsoumil519/can-duckdb-read-iceberg-v3-variant-from-amazon-s3-tables-quick-test-a191ab70bf80
--
--  The article attaches an S3 Tables catalog (ENDPOINT_TYPE s3_tables). Locally
--  we attach the same stack as exercise 1 (docker-compose.yml):
--    * RustFS     -> S3 storage for the Iceberg files
--    * Lakekeeper -> Iceberg REST catalog, metadata stored in PostgreSQL
--
--  Table created below: lake.test.gh_shred   (article: s3_tables_db.test.gh_shred)
--
--  Run via: python python/run_v3_variant_exercise.py
--  (DuckDB >= 1.5.3; the stable `iceberg` extension already reads/writes v3 —
--   the article needed core_nightly because it predates that release)
-- =============================================================================

INSTALL httpfs;  LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
INSTALL json;    LOAD json;     -- CAST(json_text AS JSON)::VARIANT

-- S3 credentials for RustFS (not needed for reads/writes when the catalog vends
-- STS credentials, but required for path-based access such as iceberg_scan on a
-- raw s3:// path).
CREATE OR REPLACE SECRET rustfs_s3 (
    TYPE s3,
    KEY_ID '${RUSTFS_ACCESS_KEY}',
    SECRET '${RUSTFS_SECRET_KEY}',
    REGION '${S3_REGION}',
    ENDPOINT '${S3_ENDPOINT}',   -- host:port only, no scheme
    URL_STYLE 'path',
    USE_SSL false
);

-- Bearer token for the unsecured local catalog (any token is accepted).
CREATE OR REPLACE SECRET lakekeeper_catalog (
    TYPE iceberg,
    TOKEN '${ICEBERG_TOKEN}'
);

ATTACH 'demo' AS lake (
    TYPE iceberg,
    ENDPOINT '${CATALOG_ENDPOINT}',   -- http://localhost:8181/catalog
    SECRET lakekeeper_catalog
);

SHOW ALL TABLES;
