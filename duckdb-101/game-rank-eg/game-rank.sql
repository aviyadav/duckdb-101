WITH ranked AS (
  SELECT
    game_id,
    region,
    day,
    player_id,
    score,
    ts,
    row_number() OVER (
      PARTITION BY game_id, region, day
      ORDER BY score DESC, ts ASC
    ) AS rn
  FROM read_parquet('leaderboard/day=2025-12-20/*.parquet')
)
SELECT *
FROM ranked
WHERE rn <= 10
ORDER BY game_id, region, day, rn;
