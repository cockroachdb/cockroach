CREATE VIEW startrek.quotes_per_season (season, quotes)
AS SELECT startrek.episodes.season, count(*)
FROM startrek.quotes
JOIN startrek.episodes
ON startrek.quotes.episode = startrek.episodes.id
GROUP BY startrek.episodes.season
