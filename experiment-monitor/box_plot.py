import os

import pandas as pd


def generate_box_plot(experiment_path, output):
    # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
    df_list = []
    for root, dirs, files in os.walk(experiment_path):
        for file in files:
            if file == "final_df.csv":
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # Identify the columns related to 'numRecordsInPerSecond'
                numRecordsInPerSecond_cols = [
                    col for col in df.columns if 'flink_taskmanager_job_task_numRecordsInPerSecond' in str(col)
                ]

                # Create a new column 'Total' that is the sum of the 'numRecordsInPerSecond' columns
                df['Total'] = df[numRecordsInPerSecond_cols].sum(axis=1)

                df_list.append(df)
    # Concatenate all dataframes
    final_df = pd.concat(df_list)

    # Group by 'parallelism' and get 'Total' values
    grouped = final_df.groupby('Parallelism')['Total']

    print(grouped)
    # Create a new dataframe for boxplot
    # boxplot_df = pd.DataFrame({col: vals['Total'] for col, vals in grouped})
    #
    # # Generate box plot
    # boxplot_df.boxplot()
    #
    # # Display the plot
    # plt.show()


if __name__ == "__main__":
    base_path = "../paper-plots"

    output_path = os.path.join(base_path, "output")

    join_kk_latency_experiments = os.path.join(base_path, "Join-kk/latency")

    generate_box_plot(join_kk_latency_experiments, output_path)
