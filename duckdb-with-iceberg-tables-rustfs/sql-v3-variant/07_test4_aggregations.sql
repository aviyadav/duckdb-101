-- =============================================================================
--  07_test4_aggregations.sql        (article: "Test 4: Aggregations at scale -> PASS")
--  SELECT count(*) AS total, count(v.org.login) AS with_org,
--         count(v.payload.action) AS with_action FROM ...;
--
--  This is the test that proves VARIANT field access is pushed down per value:
--  the counts differ because not every event has an `org` or a payload `action`.
-- =============================================================================

SELECT
    count(*)                  AS total,
    count(v.org.login)        AS with_org,
    count(v.payload.action)   AS with_action
FROM lake.test.gh_shred;
