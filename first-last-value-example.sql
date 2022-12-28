/*
 SQLite Language
 Table = endsong_subset
 Columns = ['ts', 'ms_played', 'track_name', 'album_name', 'artist_name', 'spotify_track_uri', 'reason_start',
 'reason_end', 'shuffle', 'skipped', 'offline', 'offline_timestamp', 'incognito_mode']
 */

 -- FIRST_VALUE() is a window function that returns the first value in a window partition
SELECT
    ts,
    track_name,
    artist_name,
    FIRST_VALUE(ts) OVER (PARTITION BY artist_name, track_name ORDER BY ts) AS first_played,
    IIF(FIRST_VALUE(ts) OVER (PARTITION BY artist_name, track_name ORDER BY ts) = ts, 1, 0) AS first_played_flag
       FROM endsong_subset
WHERE spotify_track_uri IS NOT NULL
ORDER BY ts DESC
LIMIT 20;

-- LAST_VALUE() is a window function that returns the last value in a window partition
SELECT
    ts,
    track_name,
    artist_name,
    LAST_VALUE(ts) OVER (PARTITION BY artist_name, track_name ORDER BY ts) AS last_played,
    IIF(LAST_VALUE(ts) OVER (PARTITION BY artist_name, track_name ORDER BY ts) = ts, 1, 0) AS last_played_flag
       FROM endsong_subset
WHERE spotify_track_uri IS NOT NULL
ORDER BY ts DESC
LIMIT 20;