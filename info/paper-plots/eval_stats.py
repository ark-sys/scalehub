import pandas as pd

exp_path = "Join-kv/no_lat/2"
# exp_path = "Map/no_lat/3"
file_name = "mean_stderr.csv"


def read_mean_stderr(file_path):
    df = pd.read_csv(file_path, index_col=0)
    return df


if __name__ == "__main__":
    file_path = f"{exp_path}/{file_name}"
    df = read_mean_stderr(file_path)

    # Add a column stating the variation of the mean between two rows, in percentage
    df["mean_variation"] = (
        (df["Mean"] - df["Mean"].shift(1)) / df["Mean"].shift(1) * 100
    )

    # Add a column showing the variation of the mean between a row and the first row, in percentage
    df["mean_variation_from_start"] = (
        (df["Mean"] - df["Mean"].iloc[1]) / df["Mean"].iloc[1] * 100
    )

    # Add a column showing how much percentage is added from previous row of mean_variation_from_start
    df["mean_variation_from_start_diff"] = df["mean_variation_from_start"] - df[
        "mean_variation_from_start"
    ].shift(1)

    avg_diff = df["mean_variation_from_start_diff"].mean()
    avg_diff_1_to_5 = df["mean_variation_from_start_diff"].iloc[1:5].mean()
    avg_diff_from_6 = df["mean_variation_from_start_diff"].iloc[5:].mean()
    print(df.to_string())
    print(f"Average added throughput with every additional instance: {avg_diff:.2f}%")
    print(
        f"Average added throughput with every additional instance from 1 to 5: {avg_diff_1_to_5:.2f}%"
    )
    print(
        f"Average added throughput with every additional instance from 6 to end: {avg_diff_from_6:.2f}%"
    )
