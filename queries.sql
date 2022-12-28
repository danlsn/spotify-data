/*
 SQLite Language
 Table = endsong_subset
 Columns = ['ts', 'ms_played', 'track_name', 'album_name', 'artist_name', 'spotify_track_uri', 'reason_start',
 'reason_end', 'shuffle', 'skipped', 'offline', 'offline_timestamp', 'incognito_mode']
 *//*
 SQLite Language
 Table = endsong_subset
 Columns = ['ts', 'ms_played', 'track_name', 'album_name', 'artist_name', 'spotify_track_uri', 'reason_start',
 'reason_end', 'shuffle', 'skipped', 'offline', 'offline_timestamp', 'incognito_mode']
 */

SELECT COUNT(*)
FROM endsong;

/*
 Create view of the top 100 most played songs
 */
DROP VIEW IF EXISTS top100;

CREATE VIEW top100 AS
SELECT spotify_track_uri, artist_name, track_name, COUNT(*) AS count
FROM endsong_subset
WHERE spotify_track_uri IS NOT NULL
GROUP BY spotify_track_uri, artist_name, track_name
ORDER BY count DESC
LIMIT 100;

/*
 Select rows in endsong_subset where spotify_track_uri is in top100
 */

SELECT *
FROM endsong_subset
WHERE spotify_track_uri IN (SELECT spotify_track_uri FROM top100);

-- Create a view of the above query
DROP VIEW IF EXISTS top100_endsong;

CREATE VIEW top100_endsong AS
SELECT *
FROM endsong_subset
WHERE spotify_track_uri IN (SELECT spotify_track_uri FROM top100)
ORDER BY ts;

/*
 Demo of DENSE_RANK() function
 */

WITH cte as (
    SELECT
        artist_name,
        track_name,
        SUM(ms_played) as total_ms_played,
        COUNT(*) as count_played
    FROM top100_endsong
    GROUP BY artist_name, track_name
    )

SELECT
    *,
    dense_rank() over (order by total_ms_played desc) as rank_ms_played,
    dense_rank() over (order by count_played desc) as rank_count_played
FROM cte
ORDER BY rank_count_played;

/*
 Create a view of artist_name, track_name, total_ms_played, count_played
 */

DROP VIEW IF EXISTS top100_artist_track_stats;

CREATE VIEW top100_artist_track_stats AS
    SELECT
        artist_name,
        track_name,
        SUM(ms_played) as total_ms_played,
        COUNT(*) as count_played
    FROM top100_endsong
    GROUP BY artist_name, track_name;

EXPLAIN QUERY PLAN
WITH cte as (
    SELECT
        artist_name,
        track_name,
        SUM(ms_played) as total_ms_played,
        COUNT(*) as count_played
    FROM top100_endsong
    GROUP BY artist_name, track_name
)

SELECT
    *,
    dense_rank() over (order by total_ms_played desc) as rank_ms_played,
    dense_rank() over (order by count_played desc) as rank_count_played
FROM cte
ORDER BY rank_count_played;


