/* I am using Postgres for following queries */

-- Query 7: Longest streak of games where LeBron James scored over 10 points
WITH with_over_10_pts AS (
  SELECT player_id
    , game_date_est
    , CASE WHEN COALESCE(pts, 0) > 10 THEN 0 ELSE 1 END AS over_10
  FROM game_details gd
    INNER JOIN games g
      ON gd.game_id = g.game_id
  WHERE player_name = 'LeBron James'
)
, with_windowed AS (
  SELECT player_id
    , game_date_est
    , over_10
    , SUM(over_10) OVER(
        PARTITION BY player_id ORDER BY game_date_est
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS streak_group_id
  FROM with_over_10_pts
)
, all_streaks AS (
  SELECT player_id
    , MIN(game_date_est) streak_start_date
    , MAX(game_date_est) streak_end_date
    , COUNT(1) AS streak
  FROM with_windowed
  GROUP BY player_id, streak_group_id
)

SELECT MAX(streak)
FROM all_streaks
;
