import json
import logging
import sqlite3
import time

import spotipy
import pandas as pd
from spotipy import SpotifyOAuth

DB = "spotify.db"
ENDSONG_TABLE = "endsong"
TRACKS_TABLE = "tracks"
TRACK_INFO_TABLE = "track_info"
TRACK_AUDIO_FEATURES_TABLE = "track_audio_features"

WRITE_TO_DB = True
WRITE_TO_JSON = True
CHECK_EXISTING_JSON = False
CHECK_EXISTING_DB = True
SLEEP_TIME = 0.001

GET_TRACK_AUDIO_FEATURES = False
GET_TRACK_INFO = True


def create_spotify_client():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read"))
    return sp


def create_logger():
    # Create logger
    gl = logging.getLogger(__name__)
    # Set logger level
    gl.setLevel(logging.INFO)
    # Create console handler and set level to info
    ch = logging.StreamHandler()
    fh = logging.FileHandler("logs/get-spotify-track-info.log")
    ch.setLevel(logging.INFO)
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    # Add formatter to ch
    ch.setFormatter(formatter)
    # Add ch to logger
    gl.addHandler(ch)
    gl.addHandler(fh)
    return gl


logger = create_logger()


def connect_to_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    return conn, c


def get_track_ids_from_db(connection, cursor, limit=None, offset=0):
    conn, c = connection, cursor
    # Get unique 'spotify_track_uri' from 'endsong' table
    # Select distinct 'spotify_track_uri' if not none from 'endsong' table offset by offset and limited by limit
    c.execute(
        f"SELECT DISTINCT spotify_track_uri FROM {ENDSONG_TABLE} WHERE spotify_track_uri IS NOT NULL LIMIT"
        f" {limit if limit else '-1'} OFFSET {offset}"
    )
    track_ids = [track_id[0] for track_id in c.fetchall()]
    return track_ids


def get_track_id_batches(connection, cursor, limit=None, offset=0):
    track_ids = get_track_ids_from_db(
        connection, cursor, limit=limit, offset=offset
    )
    # Drop any None values
    track_ids = [track_id for track_id in track_ids if track_id]
    track_id_batches = [
        track_ids[i : i + 50] for i in range(0, len(track_ids), 50)
    ]
    return track_id_batches


def get_spotify_track_audio_features(
    sp, track_id_batches, sleep_time=SLEEP_TIME
):
    track_audio_features = []
    # Loop through each batch of track IDs
    for track_ids_batch in track_id_batches:
        # Get track audio features
        track_audio_features.extend(sp.audio_features(track_ids_batch))
        # Log track audio features
        logger.info(
            f"Got track audio features for {len(track_audio_features)} tracks. Sleeping for {sleep_time} second/s."
        )
        # Sleep to avoid rate limiting
        time.sleep(sleep_time)
    # Loop through each batch of track IDs
    # for track_ids_batch in track_id_batches:
    #     for track_id in track_ids_batch:
    #         # Get track audio features
    #         track_audio_features.append(sp.audio_features(track_id)[0])
    #         # Log track audio features if len(track_audio_features) is a multiple of 10
    #         if len(track_audio_features) % 10 == 0:
    #             logger.info(
    #                 f"Got track audio features for {len(track_audio_features)} tracks"
    #             )
    #         # Sleep for 1/4 second to avoid rate limiting
    #         time.sleep(sleep_time)
    # Log that track audio features have been retrieved
    logger.info(
        f"Got track audio features for {len(track_audio_features)} tracks"
    )
    # Create a pandas DataFrame from track_audio_features
    track_audio_features_df = pd.json_normalize(track_audio_features)
    # Set index to id
    track_audio_features_df.set_index("id", inplace=True)
    return track_audio_features_df


# Fetch ids from TRACK_AUDIO_FEATURES_TABLE in DB and return as a set
def get_existing_track_ids(connection, cursor, table):
    # Log that existing track ids are being retrieved
    logger.info(f"Retrieving existing track ids from {table}")
    # Execute SQL query to retrieve ids from TRACK_AUDIO_FEATURES_TABLE
    cursor.execute(f"SELECT DISTINCT uri FROM {table}")
    # Fetch all ids from TRACK_AUDIO_FEATURES_TABLE
    ids = cursor.fetchall()
    # Log that existing track ids have been retrieved
    logger.info(f"Retrieved existing track ids from {table}")
    # Return ids as a set
    return set([id[0] for id in ids])


def table_exists(cursor, table):
    # Log that table existence is being checked
    logger.info(f"Checking if {table} exists")
    # Execute SQL query to check if table exists
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    )
    # Fetch result of SQL query
    result = cursor.fetchone()
    # Log that table existence has been checked
    logger.info(f"Checked if {table} exists")
    # Return True if table exists, else False
    return result is not None


def insert_spotify_track_audio_features(
    connection, cursor, track_audio_features_df
):
    # Get existing track ids from TRACK_AUDIO_FEATURES_TABLE, if it exists
    existing_track_ids = (
        get_existing_track_ids(connection, cursor, TRACK_AUDIO_FEATURES_TABLE)
        if table_exists(connection, cursor, TRACK_AUDIO_FEATURES_TABLE)
        else set()
    )
    # Log that existing track ids have been retrieved
    logger.info(
        f"Retrieved existing track ids from {TRACK_AUDIO_FEATURES_TABLE}"
    )
    # Log number of existing track ids
    logger.info(
        f"{len(existing_track_ids)} existing track ids retrieved from {TRACK_AUDIO_FEATURES_TABLE}"
    )

    # Get track ids from track_audio_features_df
    track_ids = set(track_audio_features_df.index)
    # Log number of track ids
    logger.info(
        f"{len(track_ids)} track ids retrieved from track_audio_features_df"
    )
    # Get track ids that are not in existing_track_ids
    new_track_ids = track_ids - existing_track_ids
    # Log number of new track ids
    logger.info(
        f"{len(new_track_ids)} new track ids retrieved from track_audio_features_df"
    )

    # Get rows from track_audio_features_df that have track ids in new_track_ids
    new_track_audio_features_df = track_audio_features_df.loc[new_track_ids]
    # Log that new track audio features are being inserted
    logger.info(
        f"Inserting {len(new_track_audio_features_df)} new track audio features"
    )
    # Insert new track audio features into TRACK_AUDIO_FEATURES_TABLE
    new_track_audio_features_df.to_sql(
        TRACK_AUDIO_FEATURES_TABLE, connection, if_exists="append",
    )
    # Log that the track audio features have been inserted into the database
    logger.info(f"Inserted track audio features into SQLite database '{DB}'")


def check_existing_json(track_info):
    # Load track_info.json if it exists, and append to track_info
    try:
        track_info = pd.read_json("track_info.json").to_dict(orient="records")
    except ValueError:
        pass
        #
    return track_info


def get_spotify_track_info(
    sp,
    track_ids_batches,
    connection,
    cursor,
    check_existing_json=CHECK_EXISTING_JSON,
    check_existing_db=CHECK_EXISTING_DB,
):
    """
    Get track info from Spotify API for each track ID in the database and write to JSON and/or SQLite database.
    Step 1: Get existing unique track IDs from database
    Step 2: Load existing track info from JSON file
    Step 3: Loop through each batch of track IDs
    Step 4: Get track info from Spotify API
    Step 5: Write track info to JSON file and/or SQLite database
    :param track_ids_batches: List of batches of track IDs
    :return: spotify_track_info_df: Pandas DataFrame of track info
    """
    # Create a set of track features, track info
    track_info = []
    # If CHECK_EXISTING_JSON is True, load existing track info from JSON file
    if check_existing_json:
        # Load track_info.json if it exists, and append items to track_info
        try:
            # Log that track_info.json is being loaded
            logger.info("Loading track_info.json")
            # Load track_info.json into a pandas DataFrame
            track_info_df = pd.read_json("data/out/track_info.json")
            # Log that track_info.json has been loaded
            logger.info(f"Loaded track_info.json with {len(track_info)} rows")
        except ValueError:
            pass

    if check_existing_db:
        # Get existing track ids from TRACK_INFO_TABLE
        existing_track_ids = get_existing_track_ids(
            connection, cursor, TRACK_INFO_TABLE
        )
        # Log that existing track ids have been retrieved
        logger.info(f"Retrieved existing track ids from {TRACK_INFO_TABLE}")
        # Log number of existing track ids
        logger.info(
            f"{len(existing_track_ids)} existing track ids retrieved from {TRACK_INFO_TABLE}"
        )
    # Loop through each batch of track IDs
    for track_ids_batch in track_ids_batches:
        # Check that track_ids in track_ids_batch are not in existing_track_ids
        if check_existing_db:
            track_ids_batch = list(set(track_ids_batch) - existing_track_ids)
        if len(track_ids_batch) > 0:
            # Get track info for batch
            track_info.extend(sp.tracks(track_ids_batch)["tracks"])
            # Log track info
            logger.info(f"Got track info for {len(track_info)} tracks")
            # Sleep for 1 second to avoid rate limiting
            time.sleep(SLEEP_TIME)
        else:
            # Log that no track info was retrieved
            logger.info("No track info retrieved")
    # Create a pandas DataFrame from track_info
    spotify_track_info_df = pd.DataFrame(track_info)
    # If track_info is not empty
    if track_info:
        # Drop track_info_df['available_markets'] column
        spotify_track_info_df.drop("available_markets", axis=1, inplace=True)
        # Drop track_info_df['disc_number'] column
        spotify_track_info_df.drop("disc_number", axis=1, inplace=True)
        # Get album_type, name, uri, release_date, release_date_precision, total_tracks from track_info_df['album']
        # column, and drop column
        spotify_track_info_df["album_type"] = spotify_track_info_df["album"].apply(
            lambda x: x["album_type"]
        )
        spotify_track_info_df["album_name"] = spotify_track_info_df["album"].apply(
            lambda x: x["name"]
        )
        spotify_track_info_df["album_uri"] = spotify_track_info_df["album"].apply(
            lambda x: x["uri"]
        )
        spotify_track_info_df["album_release_date"] = spotify_track_info_df[
            "album"
        ].apply(lambda x: x["release_date"])
        spotify_track_info_df[
            "album_release_date_precision"
        ] = spotify_track_info_df["album"].apply(
            lambda x: x["release_date_precision"]
        )
        spotify_track_info_df["album_total_tracks"] = spotify_track_info_df[
            "album"
        ].apply(lambda x: x["total_tracks"])
        # Get artist name, uri from track_info_df['artists'] column as a semicolon delimited list, and drop column
        spotify_track_info_df["artist_names"] = spotify_track_info_df[
            "artists"
        ].apply(lambda x: ";".join([artist["name"] for artist in x]))
        spotify_track_info_df["artist_uris"] = spotify_track_info_df[
            "artists"
        ].apply(lambda x: ";".join([artist["uri"] for artist in x]))
        # Drop artist, album, external_urls, preview_url, type, and href columns
        spotify_track_info_df.drop(
            [
                "preview_url",
                "type",
                "href",
                "external_urls",
                "external_ids",
                "artists",
                "album",
            ],
            axis=1,
            inplace=True,
        )
        # Set track_info_df['id'] column as index
        spotify_track_info_df.set_index("id", inplace=True)
    else:
        return None
    # Return spotify_track_info_df
    return spotify_track_info_df


def create_track_info_table(cursor, table=TRACK_INFO_TABLE):
    """
    Create TRACK_INFO_TABLE if it does not already exist.
    :param connection: SQLite database connection
    :param table: Name of table to create
    """
    # Create TRACK_INFO_TABLE if it does not already exist
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id TEXT PRIMARY KEY,
            album_type TEXT,
            album_name TEXT,
            album_uri TEXT,
            album_release_date TEXT,
            album_release_date_precision TEXT,
            album_total_tracks INTEGER,
            artist_names TEXT,
            artist_uris TEXT,
            is_local INTEGER,
            track_number INTEGER,
            duration_ms INTEGER,
            explicit INTEGER,
            popularity INTEGER,
            name TEXT,
            uri TEXT
        )
    """
    )


def insert_spotify_track_info(
    connection,
    cursor,
    spotify_track_info_df,
    track_info_table=TRACK_INFO_TABLE,
    check_existing_json=CHECK_EXISTING_JSON,
    check_existing_db=CHECK_EXISTING_DB,
):
    """
    Insert spotify_track_info_df into SQLite database.
    Step 1: Check if track info already exists in database, and drop duplicates
    Step 2: Insert track info into TRACK_INFO_TABLE
    :param spotify_track_info_df: Pandas DataFrame of track info
    :return: None
    """
    # If spotify_track_info_df is none, return
    if spotify_track_info_df is None:
        return None
    # Check if track_info_table already exists in database
    if check_existing_db:
        # Check if track_info_table already exists in database, else create it
        if not table_exists(cursor, track_info_table):
            # Create track_info_table
            create_track_info_table(cursor, track_info_table)

        # If track_info_table already exists in database, drop duplicates
        spotify_track_info_df = spotify_track_info_df[
            ~spotify_track_info_df.index.isin(
                pd.read_sql_query(
                    f"SELECT id FROM {track_info_table}", connection
                ).id
            )
        ]

    # Check if spotify_track_info_df id column is unique
    if not spotify_track_info_df.index.is_unique:
        # Drop duplicates
        spotify_track_info_df = spotify_track_info_df[
            ~spotify_track_info_df.index.duplicated(keep="first")
        ]

    # Insert track info into TRACK_INFO_TABLE
    spotify_track_info_df.to_sql(
        track_info_table, connection, if_exists="append"
    )
    # Log that track info has been inserted into TRACK_INFO_TABLE
    logger.info(
        f"Inserted {len(spotify_track_info_df)} rows into {track_info_table}"
    )
    return


def main():
    # Log that the script has started
    logger.info("Started script")

    # Create Spotipy Client
    sp = create_spotify_client()
    # Log that the Spotipy Client has been created
    logger.info("Created Spotipy Client")

    # Connect to SQLite database 'spotify.db'
    conn, c = connect_to_db()
    # Log that the connection to the database has been established
    logger.info(f"Connected to SQLite database '{DB}'")

    # Get track_id batches from database
    batches = get_track_id_batches(conn, c, limit=None, offset=0)
    # Log that the track_id batches have been retrieved
    logger.info(f"Retrieved track_id batches from database '{DB}'")

    if GET_TRACK_AUDIO_FEATURES:
        # Get audio features for tracks
        audio_features = get_spotify_track_audio_features(sp, batches)
        if WRITE_TO_DB:
            # Insert track audio features into database
            insert_spotify_track_audio_features(conn, c, audio_features)

        # Log that the audio features have been retrieved
        logger.info(
            f"Retrieved audio features for tracks from database '{DB}'"
        )

    if GET_TRACK_INFO:
        # Get track info for tracks
        track_info = get_spotify_track_info(
            sp, batches, connection=conn, cursor=c
        )
        if WRITE_TO_DB:
            # Insert track info into database
            insert_spotify_track_info(conn, c, track_info)

        # Log that the track info has been retrieved
        logger.info(f"Retrieved track info for tracks from database '{DB}'")
    # Close connection to SQLite database 'spotify.db'
    conn.close()
    # Log that the connection to the database has been closed
    logger.info(f"Closed connection to SQLite database '{DB}'")


if __name__ == "__main__":

    main()
