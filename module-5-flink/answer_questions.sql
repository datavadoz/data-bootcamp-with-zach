/*
Question: What is the average number of web events of a session from a user on Tech Creator?
Answer: 2.69
*/
SELECT ROUND(AVG(event_count)::numeric, 2) AS avg_events_per_session
FROM processed_events_aggregated
WHERE host LIKE '%.techcreator.io';

/*
Question: Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
Answer: At the time running the query, there is no event that triggered with 3 above hosts. So no data is displayed.
*/
SELECT host,
       ROUND(AVG(event_count)::numeric, 2) AS avg_events_per_session,
       COUNT(*) AS session_count
FROM processed_events_aggregated
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY host;
