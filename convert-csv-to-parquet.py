import pandas as pd
import glob

DATA_PATH = "data/pocketcasts-data-at-2022-07-26"
CSV_FILES = DATA_PATH + "/*.csv"


def convert_csv_to_parquet(csv_file):
    df = pd.read_csv(csv_file)
    df.to_parquet(csv_file.replace(".csv", ".parquet"))


def list_csv_files():
    return glob.glob(CSV_FILES)


def main():
    # Convert all CSV files to Parquet, except for the History.csv and Playlists.csv file
    for csv_file in list_csv_files():
        if "History" in csv_file or "Playlists" in csv_file:
            continue
        convert_csv_to_parquet(csv_file)


if __name__ == "__main__":
    main()
