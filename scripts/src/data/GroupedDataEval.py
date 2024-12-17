import os

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

from scripts.src.data.Plotter import Plotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class GroupedDataEval:
    skip = 30

    def __init__(self, log: Logger, multi_run_path: str):
        self.__log: Logger = log
        self.t: Tools = Tools(log)
        self.plotter = Plotter(self.__log, plots_path=multi_run_path)

        self.multi_run_path = multi_run_path
        # We retrieve the config from the first run
        exp_path = os.path.join(multi_run_path, "1", "exp_log.txt")
        self.config: Config = Config(log, exp_path)

    # Load the mean_stderr.csv file and generate a boxplot
    def generate_box_for_means(self):

        dfs = []
        for root, dirs, files in os.walk(self.multi_run_path):
            for file in files:
                if file == "mean_stderr.csv":
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    # df = df.drop(0)
                    dfs.append(df)

        num_runs = len(dfs)
        final_df = pd.concat(dfs)

        final_df = (
            final_df.groupby("Parallelism")
            .agg(
                Throughput_min=("Throughput", "min"),
                Throughput_max=("Throughput", "max"),
                Throughput_mean=("Throughput", "mean"),
            )
            .reset_index()
        )

        # Save new data df to file
        final_df.to_csv(os.path.join(self.multi_run_path, "final_df.csv"), index=False)

        boxplot_data = [
            [
                group["Throughput_min"].values[0],
                group["Throughput_mean"].values[0],
                group["Throughput_max"].values[0],
            ]
            for _, group in final_df.groupby("Parallelism")
        ]
        labels = [str(name) for name, _ in final_df.groupby("Parallelism")]
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

        ax.set_xlabel("Operator Parallelism", fontsize=24)
        ax.set_ylabel("Throughput (records/s)", fontsize=24)
        plt.xticks(fontsize=20)
        ax.tick_params(axis="y", labelsize=20)

        # Get Workload objective from logs
        workload_objective = 0
        for generator in self.config.get(Key.Experiment.Generators.generators):
            workload_objective += int(generator["num_sensors"])

        # Add straight dotted line at y=100000, This is the target objective for the experiment
        ax.axhline(
            y=workload_objective, color="r", linestyle="--", label="Workload objective"
        )

        # eval ylim based on max value in the data. Round up to the nearest 100000. ex. 310000 -> 400000
        ylim_val = (
            max(final_df["Throughput_max"].max(), workload_objective) // 100000 + 1
        ) * 100000

        ax.set_ylim(0, ylim_val)

        # Add a legend
        ax.legend(loc="upper right", fontsize=22)

        # # Get comment from config
        # comment = self.config.get(Key.Experiment.comment)
        # ax.set_title(f"{comment}", fontsize=24)

        fig.tight_layout()
        output_path = os.path.join(
            self.multi_run_path, f"boxplot_{num_runs}_runs_mean.png"
        )
        fig.savefig(output_path)
