SELECT * FROM read_json('events.ndjson') LIMIT 10;


SELECT *
FROM read_json(
  'events.ndjson',
  columns = {
    ts: 'TIMESTAMP',
    event: 'VARCHAR',
    amount: 'BIGINT',
    user: 'JSON',
    meta: 'JSON'
  }
);


WITH src AS (
  SELECT
    ts,
    user::JSON AS user_j,
    meta::JSON AS meta_j,
    event,
    amount
  FROM read_json('events.ndjson')
)
SELECT
  ts,
  json_extract_string(user_j, '$.id')::INT AS user_id,
  json_extract_string(meta_j, '$.ip') AS ip,
  event,
  amount
FROM src
WHERE event IN ('checkout', 'refund');


CREATE TABLE events_raw AS
SELECT
  ts::TIMESTAMP AS ts,
  event,
  amount::BIGINT AS amount,
  to_json(user) AS user_payload,   -- keep nested
  to_json(meta) AS meta_payload
FROM read_json('events.ndjson');

CREATE TABLE events_typed AS
SELECT
  ts,
  event,
  amount,
  (user_payload::JSON ->> '$.id')::INT AS user_id,
  (meta_payload::JSON ->> '$.ip') AS ip,
  user_payload::JSON AS user_json,     -- still keep it
  meta_payload::JSON AS meta_json
FROM events_raw;


COPY (
  SELECT
    ts, user_id, event, amount, ip
  FROM events_typed
)
TO 'events_typed.parquet' (FORMAT PARQUET);


SELECT event, count(*)
FROM 'events_typed.parquet'
WHERE ts >= TIMESTAMP '2026-01-01'
GROUP BY event
ORDER BY 2 DESC;
