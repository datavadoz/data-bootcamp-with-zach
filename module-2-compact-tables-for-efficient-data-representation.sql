CREATE TABLE user_cumulated (
  user_id TEXT,
  dates_active DATE[],
  date DATE,
  PRIMARY KEY(user_id, date)
)
;

INSERT INTO user_cumulated
WITH yesterday AS (
  SELECT *
  FROM user_cumulated
  WHERE date = DATE('2023-01-30')
)
, today AS (
  SELECT DISTINCT CAST(user_id AS TEXT) AS user_id
    , DATE(CAST(event_time AS TIMESTAMP)) AS date_active
  FROM events
  WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND user_id IS NOT NULL
)

SELECT COALESCE(t.user_id, y.user_id) AS user_id
  , CASE
      WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
      WHEN t.date_active IS NULL THEN y.dates_active
      ELSE y.dates_active || ARRAY[t.date_active]
    END AS dates_active
  , COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS date
FROM today t
  FULL OUTER JOIN yesterday y
    USING(user_id)
;

WITH users AS (
  SELECT *
  FROM user_cumulated
  WHERE date = DATE('2023-01-31')
)
, series AS (
  SELECT *
  FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS series_date
)
, place_holder_ints AS (
  SELECT
    CASE
      WHEN dates_active @> ARRAY[DATE(series_date)] THEN POW(2, 32 - (date - DATE(series_date)))
      ELSE 0
    END AS place_holder_int_value
    , *
  FROM users
  CROSS JOIN series
)

SELECT user_id
  , BIT_COUNT(CAST(CAST(SUM(place_holder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_montly_active
  , BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(place_holder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
  , BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(place_holder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id
;
