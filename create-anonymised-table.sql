/*
 SQL Language
 Table = endsong
 Columns = ['ts', 'username', 'ms_played', 'master_metadata_track_name', 'master_metadata_artist_name',
            'master_metadata_album_name', 'spotify_track_uri', 'reason_start', 'reason_end', 'shuffle', 'skipped',
            'offline', 'offline_timestamp', 'incognito_mode']
 Select all records from endsong ordered by ts ascending, and create table 'endsong_subset'
 */

CREATE TABLE endsong_subset AS
SELECT ts,
       ms_played,
       master_metadata_track_name        as track_name,
       master_metadata_album_album_name  as
                                            album_name,
       master_metadata_album_artist_name as artist_name,
       spotify_track_uri,
       reason_start,
       reason_end,
       shuffle,
       skipped,
       offline,
       offline_timestamp,
       incognito_mode
FROM endsong
ORDER BY ts ASC;