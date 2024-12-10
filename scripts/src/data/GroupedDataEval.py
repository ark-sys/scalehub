import os

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

from scripts.src.data.Plotter import Plotter
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class GroupedDataEval:
    skip = 30

    # experiments = ["Join-kk", "Join-kv", "Map"]
    # types = ["no_lat", "latency", "latency_jitter"]
    # high_latency = ["25ms", "50ms"]

    def __init__(self, log: Logger, multi_run_path: str):
        self.__log: Logger = log

        self.t: Tools = Tools(log)
        self.plotter = Plotter(self.__log, plots_path=multi_run_path)

    # Load the mean_stderr.csv file and generate a boxplot
    def generate_box_for_means(self, exp_path):

        dfs = []
        for root, dirs, files in os.walk(exp_path):
            for file in files:
                if file == "mean_stderr.csv":
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    df = df.drop(0)
                    dfs.append(df)

        num_runs = len(dfs)
        final_df = pd.concat(dfs)

        final_df = final_df.groupby("Parallelism")
        final_df = final_df.apply(lambda x: x.reset_index(drop=True))
        final_df = final_df.drop(columns="Parallelism")
        boxplot_data = [
            group["Throughput"].values for _, group in final_df.groupby(level=0)
        ]

        labels = [name for name, _ in final_df.groupby(level=0)]
        formatter = FuncFormatter(self.__log.thousands_formatter)

        fig, ax = plt.subplots(figsize=(8, 6))
        ax.boxplot(
            boxplot_data,
            labels=labels,
            showfliers=False,
            meanline=True,
            whis=0,
        )

        ax.yaxis.set_major_formatter(formatter)
        # ax.xaxis.set_major_locator(ticker.MultipleLocator(1))

        ax.set_ylim(0, 120000)
        ax.set_xlabel("Operator Parallelism", fontsize=24)
        ax.set_ylabel("Throughput (records/s)", fontsize=24)
        plt.xticks(fontsize=20)
        ax.tick_params(axis="y", labelsize=20)

        # Add straight dotted line at y=100000, This is the target objective for the experiment
        ax.axhline(y=100000, color="r", linestyle="--", label="Workload objective")

        # Add a legend
        ax.legend(loc="lower right", fontsize=22)

        # Save new data df to file
        final_df.to_csv(os.path.join(exp_path, "final_df.csv"))

        fig.tight_layout()
        output_path = os.path.join(exp_path, f"boxplot_{num_runs}_runs_mean.png")
        fig.savefig(output_path)
        # fig.show()

    # def eval_mean_with_backpressure_multiple(self):
    #     # This dict will contain
    #     data_dict = {}
    #     data_error_dict = {}
    #     for latency in self.high_latency:
    #         full_path = os.path.join(self.base_path, latency)
    #
    #         data_obj = DataEval(self.__log, full_path)
    #
    #         data, error_data = data_obj.eval_mean_with_backpressure()
    #
    #         data_dict[latency] = data
    #         data_error_dict[latency] = error_data
    #
    #         # Prepare the final dataset and error dataset
    #     dataset = {"subplot1": {}, "subplot2": {}}
    #     dataset_error = {"subplot1": {}, "subplot2": {}}
    #
    #     # Iterate over the data_dict and data_error_dict
    #     for latency, data in data_dict.items():
    #         for subplot, metrics in data.items():
    #             for metric, value in metrics.items():
    #                 # Append the latency to the metric name
    #                 new_metric = f"{metric}_{latency}"
    #                 dataset[subplot][new_metric] = value
    #
    #     for latency, data in data_error_dict.items():
    #         for subplot, metrics in data.items():
    #             for metric, value in metrics.items():
    #                 # Append the latency to the metric name
    #                 new_metric = f"{metric}_{latency}"
    #                 dataset_error[subplot][new_metric] = value
    #     # Prepare attributes for stacked plot
    #     attributes_dict = {
    #         "subplot1": {
    #             "ylabel": "Throughput\n(Records/s)",
    #             "ylim": (0, 120000),
    #             "axhline": 100000,
    #         },
    #         "subplot2": {
    #             "ylabel": "Backpressure\n(%)",
    #             "ylim": (0, 100),
    #         },
    #     }
    #
    #     # Generate stacked plot
    #     self.plotter.generate_stacked_plot_multiple_series_lazyass(
    #         dataset,
    #         dataset_error,
    #         attributes_dict=attributes_dict,
    #         xlabel="Operator Parallelism",
    #         filename="stacked_mean_with_backpressure.png",
    #     )
    #
    # def generate_box_plot_per_subtask(self, experiment_path):
    #     # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
    #     dfs = []
    #     for root, dirs, files in os.walk(experiment_path):
    #         for file in files:
    #             if file == "final_df.csv":
    #                 file_path = str(os.path.join(root, file))
    #
    #                 df = pd.read_csv(file_path)
    #
    #                 # Append the DataFrame to the list
    #                 dfs.append(df)
    #
    #     # Concatenate all the DataFrames into a single DataFrame
    #     final_df = pd.concat(dfs)
    #     throughput_cols = [
    #         col
    #         for col in final_df.columns
    #         if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
    #     ]
    #     # Prepare data for boxplot
    #     boxplot_data = [final_df[col].dropna() for col in throughput_cols]
    #     labels = [col[1] for col in throughput_cols]
    #
    #     # Create boxplot
    #     plt.boxplot(boxplot_data, labels=labels)
    #     plt.xlabel("Subtask")
    #     plt.ylabel("numRecordsInPerSecond")
    #     plt.title("Boxplot of numRecordsInPerSecond for each subtask")
    #     # plt.show()
    #
    # def generate_box_plot_per_parallelism(self, experiments_path):
    #     # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
    #     dfs = []
    #     for root, dirs, files in os.walk(experiments_path):
    #         for file in files:
    #             if file == "final_df.csv":
    #                 file_path = os.path.join(root, file)
    #                 df = pd.read_csv(file_path)
    #
    #                 # Group by 'Parallelism'. Timestamp column represents seconds. So we skip the first 60 seconds of each Paralaellism group
    #                 df = df.groupby("Parallelism").apply(lambda x: x.iloc[self.skip :])
    #
    #                 # Get back to a normal DataFrame
    #                 df = df.reset_index(drop=True)
    #
    #                 # Eval throughput
    #                 numRecordsInPerSecond_cols = [
    #                     col
    #                     for col in df.columns
    #                     if "flink_taskmanager_job_task_numRecordsInPerSecond"
    #                     in str(col)
    #                 ]
    #
    #                 # Add a new column 'Sum' to the DataFrame which is the sum of 'numRecordsInPerSecond' across all subtasks
    #                 df["Throughput"] = df[numRecordsInPerSecond_cols].sum(axis=1)
    #
    #                 # Only keep the columns 'Parallelism' and 'Throughput'
    #                 df = df[["Parallelism", "Throughput"]]
    #
    #                 # Remove rows with Parallelism = 0
    #                 df = df[df["Parallelism"] != 0]
    #                 # Append the DataFrame to the list
    #                 dfs.append(df)
    #     # Get number of files
    #     num_runs = len(dfs)
    #
    #     final_df = pd.concat(dfs)
    #
    #     # Group by 'Parallelism' and use 'Parallelism' values as index
    #     final_df = final_df.groupby("Parallelism")
    #     final_df = final_df.apply(lambda x: x.reset_index(drop=True))
    #     # Drop the 'Parallelism' column
    #     final_df = final_df.drop(columns="Parallelism")
    #     # Convert the MultiIndex dataframe into a list of arrays
    #     boxplot_data = [
    #         group["Throughput"].values for _, group in final_df.groupby(level=0)
    #     ]
    #     labels = [name for name, _ in final_df.groupby(level=0)]
    #
    #     fig, ax = plt.subplots()
    #     ax.boxplot(boxplot_data, labels=labels, showfliers=False, meanline=True)
    #     ax.set_xlabel("Operator Parallelism")
    #     ax.set_ylabel("Records per Second")
    #
    #     # Add straight dotted line at y=100000
    #     ax.axhline(y=100000, color="r", linestyle="--", label="100000")
    #
    #     # Decompose the path to get the operator name and the type of experiment
    #     experiment_path = experiments_path.split("/")
    #     experiment = experiment_path[-2]
    #     type = experiment_path[-1]
    #     # set title
    #     ax.set_title(f"{experiment} operator - {type}")
    #     # set subtitle
    #     # fig.suptitle(f"Experiment runs : {num_runs}", fontsize=12)
    #     # Save the plot
    #     output_path = os.path.join(experiments_path, f"{experiment}_{type}.png")
    #     fig.savefig(output_path)
    #     fig.show()

    def export(self):
        # self.__log.info(f"Handling export for {self.base_path}")
        pass
        # Export data and evaluate mean and stderr for each experiment
        # for experiment in self.experiments:
        #     for type in self.types:
        #         experiment_path = os.path.join(self.base_path, experiment, type)
        #         # If path exists, but folder is empty, skip
        #         if os.path.exists(experiment_path):
        #             # Get subdirectories
        #             subdirs = [
        #                 f.path for f in os.scandir(experiment_path) if f.is_dir()
        #             ]
        #             for subdir in subdirs:
        #                 data: DataEval = DataEval(self.__log, subdir)
        #                 data.export()
        #                 # data.eval_mean_stderr()
        #                 # data.eval_summary_plot()
        #                 # data.eval_plot_with_checkpoints()
        #                 # data.eval_experiment_plot()
        #                 # data.eval_mean_with_backpressure()

        # for experiment in self.experiments:
        #     for type in self.types:
        #         experiment_path = os.path.join(self.base_path, experiment, type)
        #         # If path exists, but folder is empty, skip
        #         if os.path.exists(experiment_path):
        #             if len(os.listdir(experiment_path)) == 0:
        #                 continue
        #             self.generate_box_for_means(experiment_path)
