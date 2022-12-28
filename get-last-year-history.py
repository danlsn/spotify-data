import os
import sqlite3

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util
from datetime import datetime, timedelta

# Set up your Spotify API credentials
client_id = os.environ["SPOTIFY_CLIENT_ID"]
client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]

scope = "user-read-recently-played user-top-read user-library-read"

auth_manager = SpotifyOAuth(scope=scope)
# Create a new Spotify API client
sp = spotipy.Spotify(auth_manager=auth_manager)

# Create SQLIte3 database connection
conn = sqlite3.connect("spotify.db")
c = conn.cursor()

# Create a table to store the data
c.execute(
    """CREATE TABLE IF NOT EXISTS history
                (id INTEGER PRIMARY KEY, track_id text, played_at text)"""
)

# Get the last 50 songs played
results = sp.current_user_recently_played(limit=50)

# Loop through the results and add them to the database
for item in results["items"]:
    c.execute(
        "INSERT INTO history(track_id, played_at) VALUES (?,?)",
        (item["track"]["id"], item["played_at"]),
    )
    conn.commit()

# Get the most recent 10 songs played from the database
c.execute("SELECT * FROM history ORDER BY played_at DESC LIMIT 10")
results = c.fetchall()

# Loop through the results and print them
for result in results:
    print(result)

# Close the database connection
conn.close()

# Set the start and end dates for the time range
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
current_user = sp.current_user()
# Retrieve the earliest track in your play history
results = sp.current_user_recently_played(limit=50, before=1670128987000)
playlists = sp.current_user_playlists()
next_50 = sp.current_user_playlists(offset=50)
hy_top_tracks_playlist = sp.playlist("0Cl8EY98eZ5ZEUU3V6nr8i")
hy_top_tracks = sp.playlist_tracks(playlist_id="0Cl8EY98eZ5ZEUU3V6nr8i")
pass
