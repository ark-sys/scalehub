import os

import pandas as pd
from matplotlib import pyplot as plt

skip_seconds = 60
skip = int(skip_seconds / 5)


def generate_box_plot_per_subtask(experiment_path):
    # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
    dfs = []
    for root, dirs, files in os.walk(experiment_path):
        for file in files:
            if file == "final_df.csv":
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # Append the DataFrame to the list
                dfs.append(df)

    # Concatenate all the DataFrames into a single DataFrame
    final_df = pd.concat(dfs)
    throughput_cols = [
        col
        for col in final_df.columns
        if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
    ]
    # Prepare data for boxplot
    boxplot_data = [final_df[col].dropna() for col in throughput_cols]
    labels = [col[1] for col in throughput_cols]

    # Create boxplot
    plt.boxplot(boxplot_data, labels=labels)
    plt.xlabel("Subtask")
    plt.ylabel("numRecordsInPerSecond")
    plt.title("Boxplot of numRecordsInPerSecond for each subtask")
    # plt.show()


def generate_box_plot_per_parallelism(experiments_path):
    # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
    dfs = []
    for root, dirs, files in os.walk(experiments_path):
        for file in files:
            if file == "final_df.csv":
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # Group by 'Parallelism'. Timestamp column represents seconds. So we skip the first 60 seconds of each Paralaellism group
                df = df.groupby("Parallelism").apply(lambda x: x.iloc[skip:])

                # Get back to a normal DataFrame
                df = df.reset_index(drop=True)

                # Eval throughput
                numRecordsInPerSecond_cols = [
                    col
                    for col in df.columns
                    if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
                ]

                # Add a new column 'Sum' to the DataFrame which is the sum of 'numRecordsInPerSecond' across all subtasks
                df["Throughput"] = df[numRecordsInPerSecond_cols].sum(axis=1)

                # Only keep the columns 'Parallelism' and 'Throughput'
                df = df[["Parallelism", "Throughput"]]

                # Remove rows with Parallelism = 0
                df = df[df["Parallelism"] != 0]
                # Append the DataFrame to the list
                dfs.append(df)
    # Get number of files
    num_runs = len(dfs)

    final_df = pd.concat(dfs)

    # Group by 'Parallelism' and use 'Parallelism' values as index
    final_df = final_df.groupby("Parallelism")
    final_df = final_df.apply(lambda x: x.reset_index(drop=True))
    # Drop the 'Parallelism' column
    final_df = final_df.drop(columns="Parallelism")
    # Convert the MultiIndex dataframe into a list of arrays
    boxplot_data = [
        group["Throughput"].values for _, group in final_df.groupby(level=0)
    ]
    labels = [name for name, _ in final_df.groupby(level=0)]

    fig, ax = plt.subplots()
    ax.boxplot(boxplot_data, labels=labels, showfliers=False, meanline=True)
    ax.set_xlabel("Operator Parallelism")
    ax.set_ylabel("Records per Second")

    # Add straight dotted line at y=100000
    ax.axhline(y=100000, color="r", linestyle="--", label="100000")

    # Decompose the path to get the operator name and the type of experiment
    experiment_path = experiments_path.split("/")
    experiment = experiment_path[-2]
    type = experiment_path[-1]
    # set title
    ax.set_title(f"{experiment} operator - {type}")
    # set subtitle
    fig.suptitle(f"Experiment runs : {num_runs}", fontsize=12)
    # Save the plot
    output_path = os.path.join(experiments_path, f"{experiment}_{type}.png")
    fig.savefig(output_path)
    fig.show()


if __name__ == "__main__":
    base_path = "../paper-plots"

    experiments = ["Join-kk", "Join-kv", "Map"]
    types = ["no_lat", "latency", "latency_jitter"]
    for experiment in experiments:
        for type in types:
            experiment_path = os.path.join(base_path, experiment, type)
            # If path exists, but folder is empty, skip
            if os.path.exists(experiment_path):
                if len(os.listdir(experiment_path)) == 0:
                    continue
                generate_box_plot_per_parallelism(experiment_path)
