import os

import numpy as np
import pandas as pd

from scripts.src.data.Plotter import Plotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class DataEval:
    def __init__(self, log: Logger, exp_path: str):
        self.__log = log
        self.exp_path = exp_path
        self.log_file = os.path.join(self.exp_path, "exp_log.json")
        self.plots_path = os.path.join(self.exp_path, "plots")
        os.makedirs(self.plots_path, exist_ok=True)
        self.plotter = Plotter(self.__log, plots_path=self.plots_path)
        self.conf = Config(log, self.log_file)
        self.start_skip = self.conf.get_int(Key.Experiment.output_skip_s.key)
        self.end_skip = 30
        self.final_df = pd.read_csv(
            os.path.join(self.exp_path, "final_df.csv"),
            index_col=0,
            header=[0, 1, 2],
        )

    def __load_and_format_data(self):
        df = self.final_df
        if df.columns.nlevels > 2:
            df.columns = df.columns.droplevel([1, 2])
        df["BackpressureTime"] = df.filter(
            regex="hardBackPressuredTimeMsPerSecond"
        ).mean(axis=1)
        df["BusyTime"] = df.filter(regex="busyTimeMsPerSecond").mean(axis=1)
        df["Throughput"] = df.filter(regex="numRecordsInPerSecond").sum(axis=1)
        df.index = pd.to_datetime(df.index, unit="s")
        return df

    def __filter_data(self, df):
        df_filtered = df.groupby("Parallelism").apply(
            lambda group: group[
                (
                    group.index
                    >= group.index.min() + pd.Timedelta(seconds=self.start_skip)
                )
                & (
                    group.index
                    <= group.index.max() - pd.Timedelta(seconds=self.end_skip)
                )
            ]
        )
        if len(df_filtered) < 7:
            self.__log.warning("Filtered data has less than 7 rows, skipping filter.")
            df_filtered = df
        else:
            df_filtered = df_filtered.drop(columns=["Parallelism"])
            df_filtered.reset_index(inplace=True)
        return df_filtered

    @staticmethod
    def __eval_mean_stderr(df_filtered):
        df_final = df_filtered.groupby("Parallelism")[
            ["Throughput", "BusyTime", "BackpressureTime"]
        ].agg(["mean", lambda x: np.std(x) / np.sqrt(x.count())])
        df_final.columns = [
            "Throughput",
            "ThroughputStdErr",
            "BusyTime",
            "BusyTimeStdErr",
            "BackpressureTime",
            "BackpressureTimeStdErr",
        ]
        df_final = df_final[df_final["Throughput"] > 0]
        return df_final

    def eval_mean_stderr(self):
        df = self.__load_and_format_data()
        df_filtered = self.__filter_data(df)
        df_final = self.__eval_mean_stderr(df_filtered)
        df_final.to_csv(os.path.join(self.exp_path, "mean_stderr.csv"))
        return df_final

    def eval_experiment_plot(self):
        df = self.__load_and_format_data()
        data = {
            "Throughput": df.filter(regex="numRecordsInPerSecond"),
            "BusyTime": df.filter(regex="busyTimeMsPerSecond"),
            "BackpressureTime": df.filter(regex="hardBackPressuredTimeMsPerSecond"),
        }
        ylim = (0, (data["Throughput"].max().max() // 10000 + 1) * 10000)
        ylim_dict = {
            "Throughput": ylim,
            "BusyTime": (0, 1200),
            "BackpressureTime": (0, 1200),
        }
        ylabels_dict = {
            "Throughput": "Records/s",
            "BusyTime": "ms/s",
            "BackpressureTime": "ms/s",
        }
        self.plotter.generate_stacked_plot(
            data,
            title="Experiment Plot",
            xlabel="Time (s)",
            ylabels_dict=ylabels_dict,
            ylim_dict=ylim_dict,
            filename="experiment_plot.png",
        )

    def eval_summary_plot(self):
        dataset = self.eval_mean_stderr()
        ax1_data = {"Throughput": dataset["Throughput"]}
        ax1_error_data = {"Throughput": dataset["ThroughputStdErr"]}
        ax2_data = {
            "BusyTime": dataset["BusyTime"],
            "BackpressureTime": dataset["BackpressureTime"],
        }
        ax2_error_data = {
            "BusyTime": dataset["BusyTimeStdErr"],
            "BackpressureTime": dataset["BackpressureTimeStdErr"],
        }
        # jobname = self.conf.get(Key.Experiment.job_file.key)
        # operator_name = (
        #     "Join" if "join" in jobname else "Map" if "map" in jobname else "Unknown"
        # )
        self.plotter.generate_single_frame_multiple_series_plot(
            ax1_data,
            ax1_error_data,
            ax2_data,
            ax2_error_data,
            xlabel="Parallelism Level",
            ylabels_dict={
                "Throughput": "Records/s",
                "BusyTime": "ms/s",
                "BackpressureTime": "ms/s",
            },
            ylim=(0, (dataset["Throughput"].max() // 50000 + 1) * 50000),
            ylim2=(0, 1200),
            filename="summary_plot.png",
        )
