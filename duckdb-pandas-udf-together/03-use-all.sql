WITH cleaned AS (
  SELECT
    id,
    regexp_replace(lower(price_text), '[^0-9.,]', '', 'g') AS price_digits
  FROM raw_prices
)
SELECT
  id,
  parse_money(price_digits) AS price_value
FROM cleaned;