import json
import os
import re
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from .Conf import Conf
from .Defaults import DefaultKeys as Key
from .Logger import Logger


class ExperimentData:
    metrics_dict = {
        "operator_metrics": [
            "flink_taskmanager_job_task_numRecordsInPerSecond",
            "flink_taskmanager_job_task_busyTimeMsPerSecond",
        ],
        "sources_metrics": [
            "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"
        ],
        "job_metrics": [
            "flink_jobmanager_job_lastCheckpointSize",
            "flink_jobmanager_job_lastCheckpointDuration",
        ],
    }

    def __init__(self, log: Logger, exp_path: str, force: bool = False):
        self.__log = log
        # Path to experiment folder
        self.exp_path = exp_path
        # Path to log file
        self.log_file = os.path.join(self.exp_path, "exp_log.txt")
        # Path to transscale log file
        self.transscale_log = os.path.join(self.exp_path, "transscale_log.txt")

        # Create an export folder if it doesn't exist
        self.export_path = os.path.join(self.exp_path, "export")
        if not os.path.exists(self.export_path):
            os.makedirs(self.export_path)

        # Create a folder for plots if it doesn't exist
        self.plots_path = os.path.join(self.exp_path, "plots")
        if not os.path.exists(self.plots_path):
            os.makedirs(self.plots_path)

        # Parse configuration file for experiment
        self.conf: Conf = Conf(log, log_path=self.log_file)

        self.force = force

        # Parse log file for timestamps
        self.start_ts, self.end_ts = self.__get_timestamps_from_log(self.log_file)

        # Time to skip in seconds at the beginning and the end of a parallelism region
        self.start_skip = 10
        self.end_skip = 10
        # VictoriaMetrics database url
        # self.db_url = "victoria-metrics-single-server.default.svc.cluster.local:8428"
        self.db_url = "localhost/vm"

    def __get_timestamps_from_log(self, log_path: str) -> tuple[int, int]:
        # Parse log file for experiment info
        with open(log_path, "r") as log_file:
            logs = log_file.read()
        # Extract start and end timestamps from log file
        start_ts = int(re.search(r"Experiment start at : (\d+)", logs).group(1))
        end_ts = int(re.search(r"Experiment end at : (\d+)", logs).group(1))

        # Check that timestamps are valid
        if start_ts is None or end_ts is None:
            self.__log.error("Failed to parse timestamps from log file.")
            exit(1)

        # Return timestamps
        return start_ts, end_ts

    def load_json(self, file_path) -> [dict]:
        res = []
        with open(file_path, "r") as file:
            for line in file:
                res.append(json.loads(line))
        return res

    def export_timeseries_json(
            self,
            time_series_name: str,
            format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        # Export all timeseries in native format
        output_file = os.path.join(self.export_path, f"{time_series_name}_export.json")
        if os.path.exists(output_file) and not self.force:
            self.__log.info(
                f"Skipping export of {time_series_name} timeseries: file exists."
            )
            return output_file
        else:
            api_url = f"http://{self.db_url}/api/v1/export"
            params = {
                "format": format_labels,
                "match[]": time_series_name,
                "start": self.start_ts,
                "end": self.end_ts,
            }
            import requests

            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                with open(output_file, "wb") as file:
                    file.write(response.content)
                self.__log.info(f"Data exported to {output_file}")
                return output_file
            else:
                self.__log.error(f"Error exporting data: {response.text}")

    def export_timeseries_csv(
            self,
            time_series_name: str,
            format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        output_file = os.path.join(self.export_path, f"{time_series_name}_export.csv")

        # If file exists and force is not set, skip export, otherwise export data
        if os.path.exists(output_file) and not self.force:
            self.__log.info(
                f"Skipping export of {time_series_name} timeseries: file exists."
            )
            return output_file, pd.read_csv(output_file, index_col="Timestamp")
        else:
            # VictoriaMetrics export CSV api
            api_url = f"http://{self.db_url}/api/v1/export/csv"
            params = {
                "format": format_labels,
                "match": time_series_name,
                "start": self.start_ts,
                "end": self.end_ts,
            }
            import requests

            response = requests.get(api_url, params=params)
            if response.status_code == 200:

                data = response.text

                # Save content to dataframe
                df = pd.read_csv(StringIO(data))

                # Do some cleaning: remove first column, rename second column to 'Timestamp' and set it as index, third column to 'Value' and put it under multindex named time_series_name
                df = df.drop(df.columns[0], axis=1)
                df.columns = ["Timestamp", time_series_name]
                df.set_index("Timestamp", inplace=True)

                df.sort_index(inplace=True)

                df.index = df.index - df.index.min()

                # Save dataframe to csv
                df.to_csv(output_file)
                self.__log.info(f"Data exported to {output_file}")
                return output_file, df
            else:
                self.__log.error(f"Error exporting data: {response.text}")

    # Extract metrics per subtask from a json exported metrics file from victoriametrics
    def get_metrics_per_subtask(
            self, metrics_content, metric_name, task_name
    ) -> tuple[str, pd.DataFrame]:
        data = {}
        output_file = os.path.join(self.export_path, f"{metric_name}_export.csv")
        for metric in metrics_content:
            if metric["metric"]["task_name"] == task_name:
                subtask_index = metric["metric"]["subtask_index"]
                if subtask_index not in data:
                    data[subtask_index] = []
                for value, timestamp in zip(metric["values"], metric["timestamps"]):
                    # Divide by 5000 to facilitate the join of multiple columns
                    data[subtask_index].append((round(timestamp / 5000), value))

        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(
            {
                f"{metric_name}_{k}": pd.Series(dict(v), name=f"{metric_name}_{k}")
                for k, v in data.items()
            }
        )

        # Sort the DataFrame by the timestamps
        df.sort_index(inplace=True)

        # Extract subtask indices from column names and sort columns by these indices
        df.columns = df.columns.str.extract("(\d+)", expand=False).astype(int)
        df = df.sort_index(axis=1)

        # Multindex subtask columns under the metric name
        df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])

        # Reset timestamps
        df.index = df.index * 5000
        # Add "Timestamp" to the index
        df.index.name = "Timestamp"

        # Set first three rows as pandas headers

        # Save dataframe to csv
        df.to_csv(output_file)
        return output_file, df

    def get_sources_metrics(self, metrics_content, metric_name) -> [pd.DataFrame]:
        # For a given job name, extract metrics for sources in a panda dataframe
        # If there are multiple sources, return a list of panda dataframes

        sources = set()
        for metric in metrics_content:
            if "Source" in metric["metric"]["task_name"]:
                sources.add(metric["metric"]["task_name"])
        res = []
        for source in sources:
            output_file = os.path.join(
                self.export_path, f"{metric_name}_{source}_export.csv"
            )
            data = {}
            for metric in metrics_content:
                if metric["metric"]["task_name"] == source:
                    subtask_index = metric["metric"]["subtask_index"]
                    if subtask_index not in data:
                        data[subtask_index] = []
                    for value, timestamp in zip(metric["values"], metric["timestamps"]):
                        # Divide by 5000 to facilitate the join of multiple columns
                        data[subtask_index].append((round(timestamp / 5000), value))

            # Convert the data to a pandas DataFrame
            df = pd.DataFrame(
                {
                    f"{metric_name}_{source}_{k}": pd.Series(
                        dict(v), name=f"{metric_name}_{k}"
                    )
                    for k, v in data.items()
                }
            )

            # Sort the DataFrame by the timestamps
            df.sort_index(inplace=True)

            # Extract subtask indices from column names and sort columns by these indices
            df.columns = df.columns.str.extract("(\d+)", expand=False).astype(int)
            df = df.sort_index(axis=1)

            # Multindex subtask columns under the metric name
            df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])

            # Reset timestamps
            df.index = df.index * 5000
            # Add "Timestamp" to the index
            df.index.name = "Timestamp"
            res.append(df)
            # Save dataframe to csv
            df.to_csv(output_file)

        return res

    def perf_query(self, query: str):
        # VictoriaMetrics query api
        api_url = f"http://{self.db_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": self.start_ts,
            "end": self.end_ts,
            "step": "10s",
        }

        self.__log.info(f"Performing query with parameters: {params}")

        import requests

        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            self.__log.error(f"Error querying data: {response.text}")

    def export_experiment_data(self):
        # Retrieve operator name from config file
        operator_name = self.conf.get(Key.Experiment.task_name)
        if operator_name == "TumblingEventTimeWindows":
            operator_name = "TumblingEventTimeWindows____Timestamps_Watermarks"
        operator_metrics_list = []
        # Export operator metrics
        for metric in self.metrics_dict["operator_metrics"]:
            path_to_export = self.export_timeseries_json(metric)
            if path_to_export:
                self.__log.info(f"Data exported to {path_to_export}")
                json_content = self.load_json(path_to_export)
                _, df = self.get_metrics_per_subtask(
                    json_content, metric, operator_name
                )
                operator_metrics_list.append(df)

        # Export sources metrics
        sources_metrics_list = []
        for metric in self.metrics_dict["sources_metrics"]:
            path_to_export = self.export_timeseries_json(metric)
            if path_to_export:
                self.__log.info(f"Data exported to {path_to_export}")
                json_content = self.load_json(path_to_export)
                df = self.get_sources_metrics(json_content, metric)
                sources_metrics_list.extend(df)
        job_metrics_list = []
        for metric in self.metrics_dict["job_metrics"]:
            path_to_export, df = self.export_timeseries_csv(metric)
            job_metrics_list.append(df)
        # Join the dataframes in metrics_with_subtasks_list and sources_metrics_list on Timestamp index
        operator_metrics_df = pd.concat(operator_metrics_list, axis=1)

        sources_metrics_df = pd.concat(sources_metrics_list, axis=1)
        final_df = pd.concat([operator_metrics_df, sources_metrics_df], axis=1)

        # Start time from 0
        final_df.index = (final_df.index - final_df.index.min()) / 1000

        # Set index as int
        final_df.index = final_df.index.astype(int)

        # Add df from job metrics to final_df
        job_metrics_df = pd.concat(job_metrics_list, axis=1)
        final_df = pd.concat([final_df, job_metrics_df], axis=1)

        # Add Parallelism column
        numRecordsInPerSecond_cols = [
            col
            for col in final_df.columns
            if any("numRecordsInPerSecond" in str(item) for item in col)
        ]
        final_df["Parallelism"] = final_df[numRecordsInPerSecond_cols].count(axis=1)

        self.final_df = final_df
        final_df.to_csv(os.path.join(self.exp_path, "final_df.csv"))

    def eval_mean_stderr(self):
        # Load the DataFrame
        df = self.final_df

        # Identify the columns related to 'numRecordsInPerSecond'
        numRecordsInPerSecond_cols = [
            col
            for col in df.columns
            if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
        ]

        # Add a new column 'Sum' to the DataFrame which is the sum of 'numRecordsInPerSecond' across all subtasks
        df["Sum"] = df[numRecordsInPerSecond_cols].sum(axis=1)

        # Convert the index to a DatetimeIndex
        df.index = pd.to_datetime(arg=df.index, unit="s")

        # Group by 'Parallelism'
        df_grouped = df.groupby("Parallelism")

        # For each group, remove the first self.start_skip seconds and the last self.end_skip seconds
        df_filtered = df_grouped.apply(
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

        df_filtered = df_filtered.drop(columns=["Parallelism"])
        df_filtered.reset_index(inplace=True)

        # Calculate mean and standard error
        df_final = df_filtered.groupby("Parallelism")["Sum"].agg(
            ["mean", lambda x: np.std(x) / np.sqrt(x.count())]
        )
        # Rename the columns
        df_final.columns = ["Mean", "StdErr"]

        # Extract predictions from transscale log
        predictions = self.__export_predictions()
        # Add the predictions to the DataFrame
        df_final["Predictions"] = np.nan
        for prediction in predictions:
            time, current_parallelism, target_parallelism, throughput = prediction
            df_final.loc[target_parallelism, "Predictions"] = throughput

        # Save the DataFrame to a CSV file
        df_final.to_csv(os.path.join(self.exp_path, "mean_stderr.csv"))

        return df_final

    def eval_experiment_plot(self):

        # Create vertically stacked plots sharing the x-axis (Timestamp)
        # On the first plot, plot the throughput in
        # On the second plot, plot the busytime per second
        # On the third plot, plot the backpressure time per second

        # Create a figure and a set of subplots

        dataset = self.final_df

        # Identify the columns related to numRecordsInPerSecond and busyTimePerSecond
        numRecordsInPerSecond_cols = [
            col
            for col in dataset.columns
            if any("numRecordsInPerSecond" in str(item) for item in col)
        ]
        busyTimePerSecond_cols = [
            col
            for col in dataset.columns
            if any("busyTimeMsPerSecond" in str(item) for item in col)
        ]
        hardBackPressuredTimeMsPerSecond_cols = [
            col
            for col in dataset.columns
            if any("hardBackPressuredTimeMsPerSecond" in str(item) for item in col)
        ]

        # Plot time series
        fig, axs = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

        # Plot numRecordsInPerSecond for each subtask
        for col in numRecordsInPerSecond_cols:
            subtask_index = col[-1]
            axs[0].plot(dataset.index, dataset[col], label=subtask_index)
        axs[0].set_title("numRecordsInPerSecond per subtask")
        axs[0].set_ylabel("Records/s")
        axs[0].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # Plot busyTimePerSecond for each subtask
        for col in busyTimePerSecond_cols:
            subtask_index = col[-1]
            axs[1].plot(dataset.index, dataset[col], label=subtask_index)
        axs[1].set_title("busyTimePerSecond per subtask")
        axs[1].set_ylabel("ms/s")
        axs[1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        for col in hardBackPressuredTimeMsPerSecond_cols:
            subtask_index = col[-1]
            axs[2].plot(dataset.index, dataset[col], label=subtask_index)

        axs[2].set_title("Backpressure Time")
        axs[2].set_ylabel("ms/s")
        axs[2].set_xlabel("Time (s)")
        plt.show()

        # Save the plot to a file
        plot_file = f"{self.plots_path}/experiment_plot.png"
        fig.savefig(plot_file)

    # Eval Summary Plot
    def eval_summary_plot(self):
        dataset = self.eval_mean_stderr()

        # Plot timeseries
        fig, ax = plt.subplots()

        # Plot the 'Mean' column with error bars for 'StdErr'
        ax.errorbar(
            dataset.index,
            dataset["Mean"],
            yerr=dataset["StdErr"],
            fmt="o",
            linestyle="-",
            color="b",
            capsize=4,
            label="Throughput In",
        )

        # Plot the 'Predictions' column
        ax.plot(
            dataset.index,
            dataset["Predictions"],
            linestyle="--",
            marker="o",
            color="r",
            label="Predictions",
        )

        # Calculate percentage error and add it to the plot
        percentage_error = (
                                   (dataset["Predictions"] - dataset["Mean"]) / dataset["Mean"]
                           ) * 100
        for x, y, error in zip(dataset.index, dataset["Predictions"], percentage_error):
            ax.annotate(
                f"{error:.2f}%",
                (x, y),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
            )

        # Set the title and labels
        ax.set_title("Mean numRecordsInPerSecond with StdErr")
        ax.set_xlabel("Parallelism Level")
        ax.set_ylabel("Mean numRecordsInPerSecond")

        # Save the plot to a file
        plot_file = f"{self.plots_path}/summary_plot.png"
        fig.savefig(plot_file)

    def eval_plot_with_checkpoints(self):
        # dataset = self.eval_mean_with_state_size()
        dataset = self.final_df

        # Convert lastCheckpointSize to MB
        dataset["flink_jobmanager_job_lastCheckpointSize"] = (
                dataset["flink_jobmanager_job_lastCheckpointSize"] / 1024 / 1024
        )

        # Stacked plot with numRecordsInPerSecond, lastCheckpointSize and busyTimePerSecond
        fig, axs = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
        numRecordsInPerSecond_cols = [
            col
            for col in dataset.columns
            if any("numRecordsInPerSecond" in str(item) for item in col)
        ]
        busyTimePerSecond_cols = [
            col
            for col in dataset.columns
            if any("busyTimeMsPerSecond" in str(item) for item in col)
        ]
        # Plot numRecordsInPerSecond for each subtask
        for col in numRecordsInPerSecond_cols:
            subtask_index = col[-1]
            axs[0].plot(dataset.index, dataset[col], label=subtask_index)

        axs[0].set_title("numRecordsInPerSecond per subtask")
        axs[0].set_ylabel("Records/s")
        axs[0].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # Plot busyTimePerSecond for each subtask
        for col in busyTimePerSecond_cols:
            subtask_index = col[-1]
            axs[1].plot(dataset.index, dataset[col], label=subtask_index)
        axs[1].set_title("busyTimePerSecond per subtask")
        axs[1].set_ylabel("ms/s")
        axs[1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # Plot lastCheckpointSize
        axs[2].plot(dataset.index, dataset["flink_jobmanager_job_lastCheckpointSize"])
        axs[2].set_title("lastCheckpointSize")
        axs[2].set_ylabel("MB")
        axs[2].set_xlabel("Time (s)")
        plt.show()

        # Save the plot to a file
        plot_file = f"{self.plots_path}/experiment_plot_with_checkpoints.png"
        fig.savefig(plot_file)

    def __export_predictions(self) -> list[tuple[int, int, int, float]]:
        with open(self.transscale_log, "r") as file:
            log_content = file.read()

        # Define the regular expression pattern
        pattern = r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\] \[COMBO_CTRL\] Reconf: Scale (Up|Down) (.*) from par (\d+)([\s\S]*?)\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\] \[RES_MNGR\] Re-configuring PARALLELISM[\s\S].*\n.*Target Parallelism: (\d+)"
        # Extract the matches
        predictions = []
        for match in re.finditer(pattern, log_content):
            # Extract the match
            current_parallelism = int(match.group(4))
            target_parallelism = int(match.group(6))
            prediction_block = match.group(5)
            operator = match.group(3)
            time = match.group(1)

            operator_name = self.conf.get(Key.Experiment.task_name)

            if operator_name in operator:
                throughput_pattern = rf".*par, transp: {target_parallelism}, 1 target tput: (\d+) new_tput %: (\d+\.\d+)"
                # Extract the throughput from prediction block
                throughput_match = re.search(throughput_pattern, prediction_block)
                if throughput_match:
                    throughput = float(throughput_match.group(2))
                    # time is in format "2024-02-04T12:00:00", transform it to milliseconds
                    time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S").timestamp()
                    predictions.append(
                        (
                            int(time + 3600),
                            current_parallelism,
                            target_parallelism,
                            throughput,
                        )
                    )

        return predictions
