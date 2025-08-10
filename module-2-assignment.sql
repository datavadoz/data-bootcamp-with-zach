/***************************************************************************
 * A query to deduplicate game_details from Day 1 so there's no duplicates *
 ***************************************************************************/
WITH dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) rn
  FROM game_details
)

SELECT *
FROM dedup
WHERE rn = 1;
;

/******************************************************************************
 * A DDL for an user_devices_cumulated table that has:                        *
 *  - a device_activity_datelist which tracks a users active days by          *
 *      browser_type                                                          *
 *  - data type here should look similar to MAP<STRING, ARRAY[DATE]>          *
 *    + or you could have browser_type as a column with multiple rows for     *
 *      each user (either way works, just be consistent!)                     *
 ******************************************************************************/

CREATE TABLE user_devices_cumulated (
  user_id NUMERIC,
  browser_type TEXT,
  snapshot_date DATE,
  device_activity_datelist DATE[],
  PRIMARY KEY(user_id, browser_type, snapshot_date)
)
;

/***********************************************************************
 * A cumulative query to generate device_activity_datelist from events *
 ***********************************************************************/
INSERT INTO user_devices_cumulated
WITH yesterday AS (
  SELECT user_id
    , browser_type
    , snapshot_date
    , device_activity_datelist
  FROM user_devices_cumulated
  WHERE snapshot_date = DATE('2023-01-30')
)
, today AS (
  SELECT DISTINCT CAST(user_id AS NUMERIC)  AS user_id
    , COALESCE(browser_type, 'Unknown')     AS browser_type
    , CAST(event_time AS DATE)              AS active_date
  FROM events
    LEFT JOIN devices USING (device_id)
  WHERE user_id IS NOT NULL
    AND CAST(event_time AS DATE) = DATE('2023-01-31')
)

SELECT COALESCE(y.user_id, t.user_id)                           AS user_id
  , COALESCE(y.browser_type, t.browser_type)                    AS browser_type
  , COALESCE(t.active_date, y.snapshot_date + INTERVAL '1 DAY') AS snapshot_date
  , CASE
      WHEN y.device_activity_datelist IS NULL
        THEN ARRAY[DATE(t.active_date)]
      WHEN t.active_date IS NULL
        THEN y.device_activity_datelist
      ELSE y.device_activity_datelist || ARRAY[DATE(t.active_date)]
    END AS device_activity_datelist
FROM yesterday y
  FULL OUTER JOIN today t
    ON y.user_id = t.user_id
      AND y.browser_type = t.browser_type
;

/*************************************************************************
 * A datelist_int generation query. Convert the device_activity_datelist *
 * column into a datelist_int column                                     *
 *************************************************************************/
WITH users AS (
  SELECT *
  FROM user_devices_cumulated
  WHERE snapshot_date = DATE('2023-01-31')
)
, series AS (
  SELECT *
  FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS series_date
)
, place_holder_ints AS (
  SELECT
    CASE
      WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
        THEN POW(2, 32 - (snapshot_date - DATE(series_date)))
      ELSE 0
    END AS place_holder_int_value
    , *
  FROM users
  CROSS JOIN series
)

SELECT user_id, browser_type, snapshot_date
  , CAST(SUM(place_holder_int_value) AS BIGINT) AS datelist_int
FROM place_holder_ints
GROUP BY user_id,browser_type, snapshot_date
;

/***************************************************************************
 * A DDL for hosts_cumulated table                                         *
 *  - a host_activity_datelist which logs to see which dates each host is  *
 *    experiencing any activity                                            *
 ***************************************************************************/
CREATE TABLE hosts_cumulated (
  host TEXT,
  snapshot_date DATE,
  host_activity_datelist DATE[],
  PRIMARY KEY(host, snapshot_date)
)
;

/************************************************************
 * The incremental query to generate host_activity_datelist *
 ************************************************************/
INSERT INTO hosts_cumulated
WITH yesterday AS (
  SELECT *
  FROM hosts_cumulated
  WHERE snapshot_date = DATE('2023-01-30')
)
, today AS (
  SELECT DISTINCT host          AS host
    , CAST(event_time AS DATE)  AS active_date
  FROM events
  WHERE CAST(event_time AS DATE) = DATE('2023-01-31')
)

SELECT COALESCE(y.host, t.host)                                 AS host
  , COALESCE(t.active_date, y.snapshot_date + INTERVAL '1 DAY') AS snapshot_date
  , CASE
      WHEN y.host_activity_datelist IS NULL
        THEN ARRAY[DATE(t.active_date)]
      WHEN t.active_date IS NULL
        THEN y.host_activity_datelist
      ELSE y.host_activity_datelist || ARRAY[DATE(t.active_date)]
    END AS host_activity_datelist
FROM yesterday y
  FULL OUTER JOIN today t
    USING (host)
;

/****************************************************************
 * A monthly, reduced fact table DDL host_activity_reduced      *
 *   - month                                                    *
 *   - host                                                     *
 *   - hit_array - think COUNT(1)                               *
 *   - unique_visitors array - think COUNT(DISTINCT user_id)    *
 ****************************************************************/
CREATE TABLE host_activity_reduced (
  host TEXT,
  month DATE,
  hit_array INTEGER[],
  unique_visitors INTEGER[],
  PRIMARY KEY(host, month)
);

/*********************************************************
 * An incremental query that loads host_activity_reduced *
 *********************************************************/
INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
  SELECT host
    , DATE(event_time)        AS active_date
    , COUNT(1)                AS hit_num
    , COUNT(DISTINCT user_id) AS unique_visitor_num
  FROM events
  WHERE DATE(event_time) = DATE('2023-01-31')
  GROUP BY host, DATE(event_time)
)
, yesterday AS (
  SELECT *
  FROM host_activity_reduced
  WHERE month = DATE('2023-01-01')
)

SELECT COALESCE(da.host, y.host)                            AS host
  , COALESCE(y.month, DATE_TRUNC('MONTH', da.active_date))  AS month
  , CASE
      WHEN y.hit_array IS NOT NULL THEN y.hit_array || ARRAY[COALESCE(da.hit_num, 0)]
      WHEN y.hit_array IS NULL
        THEN ARRAY_FILL(0, ARRAY[COALESCE(da.active_date - DATE(DATE_TRUNC('MONTH', da.active_date)), 0)]) || ARRAY[COALESCE(da.hit_num, 0)]
    END AS hit_array

  , CASE
      WHEN y.unique_visitors IS NOT NULL THEN y.unique_visitors || ARRAY[COALESCE(da.unique_visitor_num, 0)]
      WHEN y.unique_visitors IS NULL
        THEN ARRAY_FILL(0, ARRAY[COALESCE(da.active_date - DATE(DATE_TRUNC('MONTH', da.active_date)), 0)]) || ARRAY[COALESCE(da.unique_visitor_num, 0)]
    END AS unique_visitors
FROM daily_aggregate da
  FULL OUTER JOIN yesterday y
    ON da.host = y.host
ON CONFLICT (host, month)
DO UPDATE
  SET hit_array = EXCLUDED.hit_array
    , unique_visitors = EXCLUDED.unique_visitors
;
