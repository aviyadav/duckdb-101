-- =============================================================================
--  16_ingest_from_json_file.sql
--  Ingestion path for *real* semi-structured data: read newline-delimited JSON
--  from disk (or any DuckDB JSON source) and write it into an Iceberg v3
--  VARIANT column.
--
--  This is exactly how you would load the real GitHub Archive: download
--  https://data.gharchive.org/2026-05-23-22.json.gz and point ${GH_JSON_PATH}
--  at the file (DuckDB reads .gz natively). The bundled sample file has the
--  same shape, so the command below works offline.
-- =============================================================================

DROP TABLE IF EXISTS lake.test.gh_from_file;

CREATE TABLE lake.test.gh_from_file (
    id         BIGINT,
    v          VARIANT,
    created_at TIMESTAMP_NS
)
WITH ('format-version' = 3);

INSERT INTO lake.test.gh_from_file
SELECT
    row_number() OVER ()                       AS id,
    to_json(e)::VARIANT                        AS v,          -- struct -> JSON -> VARIANT
    -- read_json_auto usually infers GH Archive `created_at` as TIMESTAMP; the
    -- fallback keeps working if it comes back as a string instead.
    coalesce(
        TRY_CAST(e.created_at AS TIMESTAMP_NS),
        TRY_CAST(try_strptime(e.created_at::VARCHAR, '%Y-%m-%dT%H:%M:%SZ') AS TIMESTAMP_NS)
    )                                          AS created_at
FROM read_json_auto('${GH_JSON_PATH}') AS e;

SELECT id,
       variant_typeof(v)      AS variant_type,
       v.type::VARCHAR        AS event_type,
       v.actor.login::VARCHAR AS actor,
       created_at
FROM lake.test.gh_from_file
ORDER BY id;
