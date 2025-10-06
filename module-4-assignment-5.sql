/* I am using Postgres for following queries */

-- Query 5: Team with the most total wins
WITH team_games AS (
  SELECT g.game_id, g.home_team_id AS team_id, (g.home_team_wins = 1) AS win
  FROM games g
  UNION ALL
  SELECT g.game_id, g.visitor_team_id AS team_id, (g.home_team_wins = 0) AS win
  FROM games g
)

SELECT team_id, SUM(win::int) AS total_wins
FROM team_games
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1
;
