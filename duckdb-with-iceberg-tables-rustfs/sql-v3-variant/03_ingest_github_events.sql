-- =============================================================================
--  03_ingest_github_events.sql
--  Load GitHub-Archive-shaped events into the v3 table.
--
--  The article's table (s3_tables_db.test.gh_shred) was filled by an external
--  GitHub Archive pipeline. Locally we generate the same *shape* of data
--  (PushEvent / WatchEvent / IssuesEvent / ... with actor, org, repo, payload)
--  so the four article tests can be reproduced deterministically and offline.
--
--  Key trick: JSON text is converted with
--      CAST(<json text> AS JSON)::VARIANT
--  A plain `CAST(text AS VARIANT)` would store the *string*, not an object!
--
--  Data note: some events have `org`, others do not; some payloads have
--  `action`, others do not. That is what makes the article's aggregation test
--  (count(*) vs count(v.org.login) vs count(v.payload.action)) meaningful.
-- =============================================================================

INSERT INTO lake.test.gh_shred (id, v, created_at)
SELECT t.i, CAST(t.js AS JSON)::VARIANT, t.ts
FROM (VALUES
    (1,  '{"type":"PushEvent","created_at":"2026-05-23T22:31:07Z","actor":{"id":101,"login":"alice"},"repo":{"name":"duckdb/duckdb"},"payload":{"size":2,"ref":"refs/heads/main"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:31:07.123456789'),
    (2,  '{"type":"WatchEvent","created_at":"2026-05-23T22:32:11Z","actor":{"id":102,"login":"bob"},"repo":{"name":"apache/iceberg"},"payload":{"action":"started"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:32:11.000000001'),
    (3,  '{"type":"IssuesEvent","created_at":"2026-05-23T22:33:02Z","actor":{"id":103,"login":"carol"},"repo":{"name":"rustfs/rustfs"},"org":{"id":900,"login":"rustfs"},"payload":{"action":"opened","issue":{"number":412}},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:33:02.987654321'),
    (4,  '{"type":"PullRequestEvent","created_at":"2026-05-23T22:34:45Z","actor":{"id":104,"login":"dave"},"repo":{"name":"lakekeeper/lakekeeper"},"org":{"id":901,"login":"lakekeeper"},"payload":{"action":"closed","pull_request":{"number":88,"merged":true}},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:34:45.000000002'),
    (5,  '{"type":"CreateEvent","created_at":"2026-05-23T22:35:30Z","actor":{"id":105,"login":"erin"},"repo":{"name":"duckdb/duckdb-iceberg"},"org":{"id":902,"login":"duckdb"},"payload":{"ref_type":"branch","ref":"v3-variant"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:35:30.111222333'),
    (6,  '{"type":"ReleaseEvent","created_at":"2026-05-23T22:36:10Z","actor":{"id":106,"login":"frank"},"repo":{"name":"rustfs/rustfs"},"org":{"id":900,"login":"rustfs"},"payload":{"action":"published","release":{"tag_name":"v1.0.0"}},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:36:10.000000003'),
    (7,  '{"type":"IssueCommentEvent","created_at":"2026-05-23T22:37:52Z","actor":{"id":107,"login":"grace"},"repo":{"name":"apache/iceberg"},"org":{"id":903,"login":"apache"},"payload":{"action":"created","comment":{"id":5150}},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:37:52.444555666'),
    (8,  '{"type":"DeleteEvent","created_at":"2026-05-23T22:38:20Z","actor":{"id":108,"login":"heidi"},"repo":{"name":"example/demo"},"org":{"id":904,"login":"example"},"payload":{"ref_type":"branch","ref":"old"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:38:20.000000004'),
    (9,  '{"type":"MemberEvent","created_at":"2026-05-23T22:39:05Z","actor":{"id":109,"login":"ivan"},"repo":{"name":"lakekeeper/lakekeeper"},"org":{"id":901,"login":"lakekeeper"},"payload":{"action":"added"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:39:05.777888999'),
    (10, '{"type":"ForkEvent","created_at":"2026-05-23T22:40:33Z","actor":{"id":110,"login":"judy"},"repo":{"name":"duckdb/duckdb"},"payload":{"forkee":{"full_name":"judy/duckdb"}},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:40:33.000000005'),
    (11, '{"type":"PublicEvent","created_at":"2026-05-23T22:41:18Z","actor":{"id":111,"login":"karl"},"repo":{"name":"opensource/data"},"payload":{},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:41:18.000000006'),
    (12, '{"type":"PushEvent","created_at":"2026-05-23T22:42:44Z","actor":{"id":112,"login":"lena"},"repo":{"name":"rustfs/rustfs"},"org":{"id":900,"login":"rustfs"},"payload":{"size":5,"ref":"refs/heads/main"},"public":true}',
         TIMESTAMP_NS '2026-05-23 22:42:44.000000007')
) AS t(i, js, ts);

-- 12 events, 8 with an org, 6 with a payload action.
SELECT count(*) AS ingested,
       count(v.org.login) AS with_org,
       count(v.payload.action) AS with_action
FROM lake.test.gh_shred;
