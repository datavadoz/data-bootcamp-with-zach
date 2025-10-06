/* I am using Postgres for following queries */

-- Query 6: What is the most games a team has won in a 90 game stretch?
WITH games_augmented AS (
  SELECT gd.game_id        AS game_id
    , gd.player_id         AS player_id
    , gd.team_id           AS team_id
    , g.home_team_id       AS home_team_id
    , g.visitor_team_id    AS visitor_team_id
    , g.home_team_wins     AS home_team_wins
    , g.game_date_est      AS game_date_est
    , g.season             AS season
    , CASE
        WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN TRUE
        WHEN gd.team_id <> g.home_team_id AND home_team_wins = 0 THEN TRUE
        ELSE FALSE
      END AS win
    , COALESCE(gd.pts, 0)  AS points
  FROM game_details gd
    INNER JOIN games g
      ON gd.game_id = g.game_id
)
, aggregated AS (
  SELECT DISTINCT team_id, game_date_est, win
  FROM games_augmented
)
, windowned AS (
  SELECT team_id
    , game_date_est
    , win
    , COUNT(CASE WHEN win = TRUE THEN 1 ELSE NULL END) OVER(
        PARTITION BY team_id ORDER BY game_date_est
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ) AS most_wins_in_90_games
  FROM aggregated
)

SELECT team_id, MAX(most_wins_in_90_games) AS most_wins_in_90_games
FROM windowned
GROUP BY team_id
ORDER BY 2 DESC
;
