# load csv file in pandas. print variation in percentage between each row of column throughput. At the end evaluate the average variation.
import pandas as pd


def load_csv_file(file_path):
    return pd.read_csv(file_path)


def calculate_variation(df):
    df["variation"] = df["Throughput_mean"].pct_change() * 100
    return df


def evaluate_mean_variation(df):
    return df["variation"].mean()


def main(file_path):
    df = load_csv_file(file_path)
    df = calculate_variation(df)
    mean_variation = evaluate_mean_variation(df)
    print(df)
    print(f"Mean variation: {mean_variation}")


if __name__ == "__main__":
    # Take path from argument
    import sys

    main(sys.argv[1])
