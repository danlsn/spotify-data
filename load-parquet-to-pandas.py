import pandas as pd
import glob

DATA_PATH = "data/pocketcasts-data-at-2022-07-26"
PARQUET_FILES = DATA_PATH + "/*.parquet"


def load_parquet_to_pandas(parquet_files):
    # Load each parquet file into a list of pandas dataframes with the filename as a column
    dfs = {}
    for parquet_file in parquet_files:
        df = pd.read_parquet(parquet_file)
        # Split the filename from the path
        filename = parquet_file.split("/")[-1]
        df["filename"] = filename
        dfs[filename] = df
    return dfs


def list_parquet_files():
    return glob.glob(PARQUET_FILES)


def main():
    dfs = load_parquet_to_pandas(list_parquet_files())
    # Print the first 5 rows of each dataframe
    for df in dfs.values():
        print(df.head())

    # Set index for dfs['Episodes.parquet'] to 'uuid'
    dfs["Episodes.parquet"].set_index("uuid", inplace=True)


    # Print the first 5 rows of dfs['Podcasts.parquet']
    print(dfs["Podcasts.parquet"].head())

    # # Join dfs[History.parquet'] with dfs['Podcasts.parquet'] on 'podcast'
    # joined_df = dfs['History.parquet'].join(dfs['Podcasts.parquet'], on='podcast', how='left', lsuffix='_podcast',
    #                                         rsuffix='_episodes')

    # Inner join dfs['History.parquet'] on 'podcast' with dfs['Podcasts.parquet'] on 'uuid'
    joined_df = dfs["History.parquet"].join(
        dfs["Podcasts.parquet"],
        on="podcast",
        how="inner",
        lsuffix="_podcast",
        rsuffix="_episodes",
    )

    # Print the first 5 rows of joined_df
    print(joined_df.head())


if __name__ == "__main__":
    main()
