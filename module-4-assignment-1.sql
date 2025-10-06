/* I am using Postgres for following queries */

/* Schema of player_seasons:
    player_name text
    age integer
    height text
    weight integer
    college text
    country text
    draft_year text
    draft_round text
    draft_number text
    gp real
    pts real
    reb real
    ast real
    netrtg real
    oreb_pct real
    dreb_pct real
    usg_pct real
    ts_pct real
    ast_pct real
    season integer
*/

--Query 1: State change tracking for players
WITH active AS (
  SELECT DISTINCT player_name, season
  FROM player_seasons
)
, bounds AS (
  SELECT player_name
    , MIN(season) AS min_season
    , MAX(season) AS max_season
  FROM active
  GROUP BY player_name
)
, all_player_seasons AS (
  SELECT b.player_name
    , s AS season
  FROM bounds b
    CROSS JOIN LATERAL generate_series(b.min_season, b.max_season) AS s
)
, with_flags AS (
  SELECT aps.player_name
    , aps.season
    , a.player_name IS NOT NULL AS is_active
  FROM all_player_seasons aps
    LEFT JOIN active a
      ON a.player_name = aps.player_name AND a.season = aps.season
)
, with_windows AS (
  SELECT player_name
    , season
    , is_active
    -- Last season this player was active up to current season
    , MAX(CASE WHEN is_active THEN season END) OVER (
        PARTITION BY player_name ORDER BY season
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS last_active_season
    -- Whether previous season was active
    , LAG(is_active) OVER (PARTITION BY player_name ORDER BY season) AS prev_active
    , MIN(CASE WHEN is_active THEN season END) OVER (PARTITION BY player_name) AS first_active_season
  FROM with_flags
)
SELECT player_name
  , is_active
  , CASE
      WHEN is_active AND last_active_season IS NULL THEN 'New'
      WHEN is_active AND season - last_active_season = 1 THEN 'Continued Playing'
      WHEN is_active AND season - last_active_season > 1 THEN 'Returned from Retirement'
      WHEN NOT is_active AND prev_active = TRUE THEN 'Retired'
      WHEN NOT is_active AND (prev_active = FALSE OR prev_active IS NULL) THEN 'Stayed Retired'
    END AS player_state
  , first_active_season
  , last_active_season
  , season
FROM with_windows
ORDER BY player_name, season
;
