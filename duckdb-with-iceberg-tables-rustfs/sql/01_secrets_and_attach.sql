-- =============================================================================
--  01_secrets_and_attach.sql
--  Wire DuckDB up to the two local services started by docker-compose.yml:
--    * RustFS    (S3 API on http://localhost:9000)   -> where Iceberg files live
--    * Lakekeeper(Iceberg REST on http://localhost:8181) -> catalog metadata,
--                                                          stored in PostgreSQL
--
--  Run via: python python/run_merge_exercise.py      (recommended: the runner
--           substitutes the placeholders below from your environment)
--  Or copy this file, replace the placeholders by hand, then run:
--           duckdb :memory: < sql/01_secrets_and_attach.sql
-- =============================================================================

-- DuckDB >= 1.5.3 is required for MERGE INTO against Iceberg tables.
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

-- -----------------------------------------------------------------------------
-- 1. S3 credentials for RustFS. On AWS this came from the credential_chain;
--    locally we point DuckDB straight at the RustFS endpoint. RustFS only
--    speaks plain HTTP and path-style addressing here, hence USE_SSL false and
--    URL_STYLE 'path'.
--
--    NOTE: the endpoint is "host.docker.internal:9000" rather than
--    "localhost:9000" because Lakekeeper hands DuckDB the storage endpoint from
--    the warehouse profile (POST /tables/{table}/credentials returns
--    s3.endpoint), and that one value has to work from inside the catalog
--    container as well as from your host. Docker Desktop resolves
--    host.docker.internal in both places.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SECRET rustfs_s3 (
    TYPE s3,
    KEY_ID '${RUSTFS_ACCESS_KEY}',
    SECRET '${RUSTFS_SECRET_KEY}',
    REGION '${S3_REGION}',
    ENDPOINT '${S3_ENDPOINT}',   -- host:port only, no scheme (default: host.docker.internal:9000)
    URL_STYLE 'path',
    USE_SSL false
);

-- -----------------------------------------------------------------------------
-- 2. Credentials for the Iceberg REST catalog. The local Lakekeeper runs
--    unsecured, so any bearer token is accepted ("dummy" is what the Lakekeeper
--    docs themselves use for unsecured deployments).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SECRET lakekeeper_catalog (
    TYPE iceberg,
    TOKEN '${ICEBERG_TOKEN}'
);

-- -----------------------------------------------------------------------------
-- 3. Attach the catalog. 'demo' is the warehouse name registered in Lakekeeper
--    by config/create-warehouse.json (Postgres table `public.warehouse` in the
--    `iceberg` database).
--    Lakekeeper serves its Iceberg REST API under the /catalog base path, so the
--    endpoint is http://localhost:8181/catalog (http://localhost:8181/v1/config
--    would 404).
-- -----------------------------------------------------------------------------
ATTACH 'demo' AS lake (
    TYPE iceberg,
    ENDPOINT '${CATALOG_ENDPOINT}',
    SECRET lakekeeper_catalog
);

-- Sanity check: the catalog is reachable and (still) empty.
SHOW ALL TABLES;
