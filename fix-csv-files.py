import glob
import re

CSV_FILES = "data/pocketcasts-data-at-2022-07-26/Up Next.csv"


def fix_csv_file():
    # Load CSV file
    with open(CSV_FILES, "r") as f:
        lines = f.readlines()
        # Remove trailing newline
        lines = [line.rstrip() for line in lines]
    # Store the header
    header = lines[0]
    # Replace commas in the header with tabs
    header = header.replace(",", "\t")
    # Remove the header from the lines
    lines = lines[1:]

    # For each line split by commas 4 times into a list
    lines = [line.split(",", 4) for line in lines]

    # For each line in the list, parse into two parts using regex and drop
    pat = re.compile(r"(.*),(https?://.*)")
    # Split the 5th element into two parts using regex pattern pat
    lines = [line[:4] + pat.split(line[4]) for line in lines]
    # For each element in the list, remove element if it is empty
    lines = [[element for element in line if element] for line in lines]

    # Write to new TSV file with header and lines
    with open("data/pocketcasts-data-at-2022-07-26/Up Next.tsv", "w") as f:
        f.write(header + "\n")
        for line in lines:
            f.write("\t".join(line) + "\n")


def load_tsv_to_pandas():
    import pandas as pd

    df = pd.read_csv(
        "data/pocketcasts-data-at-2022-07-26/Up Next.tsv", sep="\t"
    )

    # Convert "modified at" millisecond timestamp to datetime
    df["modified at"] = pd.to_datetime(df["modified at"], unit="ms")
    # Convert "published at" timestamp to datetime
    df["published at"] = pd.to_datetime(df["published at"], unit="ms")
    # Order by "modified at" datetime descending
    df = df.sort_values(by="modified at", ascending=False)

    # Save as parquet file
    df.to_parquet("data/pocketcasts-data-at-2022-07-26/Up Next.parquet")

    # Read parquet file back into pandas new dataframe
    df = pd.read_parquet("data/pocketcasts-data-at-2022-07-26/Up Next.parquet")
    # Print the first 5 rows
    print(df.head())


if __name__ == "__main__":
    fix_csv_file()
    load_tsv_to_pandas()
