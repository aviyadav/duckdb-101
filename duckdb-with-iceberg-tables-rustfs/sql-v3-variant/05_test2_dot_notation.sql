-- =============================================================================
--  05_test2_dot_notation.sql        (article: "Test 2: Extract fields with dot
--                                    notation -> PASS")
--  Same query as the article: dot notation into the VARIANT, cast to VARCHAR.
--  Rows without `org` / `payload.action` return NULL, as in the article.
-- =============================================================================

SELECT
    id,
    v.type::VARCHAR        AS event_type,
    v.created_at::VARCHAR  AS created_at,
    v.actor.login::VARCHAR AS actor,
    v.org.login::VARCHAR   AS org
FROM lake.test.gh_shred
ORDER BY id
LIMIT 10;
