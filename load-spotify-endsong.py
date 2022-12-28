import pandas as pd
import glob

ENDSONG_PATH = "data/spotify-extended-data-at-2022-02-08/MyData"
ENDSONG_FILES = ENDSONG_PATH + "/endsong*.json"


def load_endsong_to_pandas(endsong_files):
    # Load each endsong file into a list of pandas dataframes with the filename as a column
    dfs = {}
    for endsong_file in endsong_files:
        df = pd.read_json(endsong_file)
        # Split the filename from the path
        filename = endsong_file.split("/")[-1]
        df["filename"] = filename
        dfs[filename] = df
    return dfs


def list_endsong_files():
    return glob.glob(ENDSONG_FILES)


def main():
    dfs = load_endsong_to_pandas(list_endsong_files())
    # Union all the dataframes in the dfs dictionary
    endsong_df = pd.concat(dfs.values())
    # Convert endsong_df['ts'] to datetime
    endsong_df["ts"] = pd.to_datetime(endsong_df["ts"])
    # Convert endsong_df['offline_timestamp'] milliseconds to Null if 0
    endsong_df["offline_timestamp"] = endsong_df["offline_timestamp"].apply(
        lambda x: None if x == 0 else x
    )
    # Convert endsong_df['offline_timestamp'] milliseconds to datetime
    endsong_df["offline_timestamp"] = pd.to_datetime(
        endsong_df["offline_timestamp"], unit="ms"
    )
    # Print the first 5 rows of endsong_df
    print(endsong_df.head())

    # Write to SQLite database 'spotify.db'
    endsong_df.to_sql('endsong', 'sqlite:///spotify.db', if_exists='replace')




if __name__ == "__main__":
    main()
