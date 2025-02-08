import json
import os
from io import StringIO

import pandas as pd
import requests

from scripts.utils.Config import Config
from scripts.utils.Defaults import (
    DefaultKeys as Key,
    MAP_PIPELINE_DICT,
    JOIN_PIPELINE_DICT,
    metrics_dict,
)
from scripts.utils.Logger import Logger


class DataExporter:
    def __init__(self, log: Logger, exp_path: str, force: bool = False):
        self.__log = log

        self.force = force

        # Path to experiment folder
        self.exp_path = exp_path

        # Export timeseries data to this path
        self.export_path = os.path.join(self.exp_path, "export")
        if not os.path.exists(self.export_path):
            os.makedirs(self.export_path)

        # Load configuration and timestamps from log file
        log_file = os.path.join(self.exp_path, "exp_log.json")
        self.config = Config(log, log_file)

        # try to get timestamps from log file
        try:
            with open(log_file, "r") as log_file:
                logs = json.load(log_file)
                self.start_ts = logs["timestamps"]["start"]
                self.end_ts = logs["timestamps"]["end"]
        except Exception as e:
            self.__log.error(f"Failed to load json file {log_file} due to : {str(e)}")
            raise e

        # VictoriaMetrics database url
        self.db_url = "victoria-metrics-single-server.default.svc.cluster.local:8428"
        self.db_url_local = "localhost/vm"

    def load_json(self, file_path) -> [dict]:
        res = []
        try:
            with open(file_path, "r") as file:
                for line in file:
                    res.append(json.loads(line))
            return res
        except Exception as e:
            self.__log.warning(
                f"Failed to load json file {file_path} due to : {str(e)}\nTrying fix..."
            )
            # Probably the file is not properly formatted, try to remove newlines and load it again
            try:
                with open(file_path, "r") as file:
                    content = file.read().replace("\n", "")

                    content = content.replace("}{", "}\n{")
                    res = [json.loads(line) for line in content.split("\n")]
                    return res

            except Exception as e:
                self.__log.error(
                    f"Failed to load json file {file_path} due to : {str(e)}"
                )
                raise e

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
            json_url = f"http://{self.db_url}/api/v1/export"
            params = {
                "format": format_labels,
                "match[]": time_series_name,
                "start": self.start_ts,
                "end": self.end_ts,
            }

            try:
                response = requests.get(json_url, params=params)
                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                self.__log.warning(
                    f"Failed to connect to VictoriaMetrics at {self.db_url}: {str(e)}"
                )
                self.__log.info(
                    f"Trying to connect to local VictoriaMetrics at {self.db_url_local}"
                )
                json_url = f"http://{self.db_url_local}/api/v1/export"
                response = requests.get(json_url, params=params)
                response.raise_for_status()

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
            csv_url = f"http://{self.db_url}/api/v1/export/csv"
            params = {
                "format": format_labels,
                "match": time_series_name,
                "start": self.start_ts,
                "end": self.end_ts,
            }
            try:
                response = requests.get(csv_url, params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.__log.warning(
                    f"Failed to connect to VictoriaMetrics at {self.db_url}: {str(e)}"
                )
                self.__log.info(
                    f"Trying to connect to local VictoriaMetrics at {self.db_url_local}"
                )
                csv_url = f"http://{self.db_url_local}/api/v1/export/csv"
                response = requests.get(csv_url, params=params)
                response.raise_for_status()
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

    def perf_query(self, query: str):
        # VictoriaMetrics query api
        query_url = f"http://{self.db_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": self.start_ts,
            "end": self.end_ts,
            "step": "10s",
        }

        self.__log.info(f"Performing query with parameters: {params}")

        try:
            response = requests.get(query_url, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.__log.warning(
                f"Failed to connect to VictoriaMetrics at {self.db_url}: {str(e)}"
            )
            self.__log.info(
                f"Trying to connect to local VictoriaMetrics at {self.db_url_local}"
            )
            query_url = f"http://{self.db_url_local}/api/v1/query_range"
            response = requests.get(query_url, params=params)
            response.raise_for_status()
        if response.status_code == 200:
            return response.json()
        else:
            self.__log.error(f"Error querying data: {response.text}")

    # Extract metrics per subtask from a json exported metrics file from victoriametrics
    def get_metrics_per_subtask(
        self, metrics_content, metric_name, task_name
    ) -> tuple[str, pd.DataFrame]:
        data = {}
        # pod_names = self.extract_pod_names(metrics_content)
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
        df.columns = (
            df.columns.astype(str).str.extract(r"(\d+)", expand=False).astype(int)
        )
        df = df.sort_index(axis=1)

        # Multindex subtask columns under the metric name
        df.columns = pd.MultiIndex.from_product(
            [[metric_name], [task_name], df.columns]
        )

        # Reset timestamps
        df.index = df.index * 5000
        # Add "Timestamp" to the index
        df.index.name = "Timestamp"

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
            df.columns = df.columns.str.extract(r"(\d+)", expand=False).astype(int)
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

    def export(self):
        ############################ Retrieve job information ############################

        # Get job name from config file
        job_name = self.config.get(Key.Experiment.name.key)
        # If job name is Map, get list of tasks from pipeline dict
        if job_name == "Map":
            tasks = list(MAP_PIPELINE_DICT.values())
        # If job name is Join, get list of tasks from pipeline dict
        elif job_name == "Join":
            tasks = list(JOIN_PIPELINE_DICT.values())
            # list contains a tuple, unpack it
            for task in tasks:
                if isinstance(task, tuple):
                    tasks.pop(tasks.index(task))
                    tasks.extend(task)
        else:
            self.__log.error(f"Unknown job name: {job_name}")
            exit(1)
        # Retrieve operator name from config file
        operator_name = self.config.get(Key.Experiment.task_name.key)
        if operator_name == "TumblingEventTimeWindows":
            operator_name = "TumblingEventTimeWindows____Timestamps_Watermarks"

        ############################ Export monitored operator metrics ############################
        operator_metrics_list = []
        # Export operator metrics
        for metric in metrics_dict["operator_metrics"]:
            path_to_export = self.export_timeseries_json(metric)
            if path_to_export:
                self.__log.info(f"Data exported to {path_to_export}")
                json_content = self.load_json(path_to_export)
                _, df = self.get_metrics_per_subtask(
                    json_content, metric, operator_name
                )
                operator_metrics_list.append(df)

        ############################ Export sources metrics ############################
        # Export sources metrics
        sources_metrics_list = []
        for metric in metrics_dict["sources_metrics"]:
            path_to_export = self.export_timeseries_json(metric)
            if path_to_export:
                self.__log.info(f"Data exported to {path_to_export}")
                json_content = self.load_json(path_to_export)
                df = self.get_sources_metrics(json_content, metric)
                sources_metrics_list.extend(df)

        ############################ Export state metrics ############################
        state_metrics_list = []
        for metric in metrics_dict["state_metrics"]:
            path_to_export, df = self.export_timeseries_csv(metric)
            state_metrics_list.append(df)

        ############################ Export metrics for all operators ############################
        job_metrics_list = []
        for metric in metrics_dict["job_metrics"]:
            path_to_export = self.export_timeseries_json(metric)
            if path_to_export:
                self.__log.info(f"Data exported to {path_to_export}")
                json_content = self.load_json(path_to_export)
                for task_name in tasks:
                    _, df = self.get_metrics_per_subtask(
                        json_content, metric, task_name
                    )

                    # Output df has multindex (metric_name, subtask_index). We want to extend the index with the task name, so that we have (metric_name, task_name, subtask_index)
                    job_metrics_list.append(df)
        # Join the dataframes in metrics_with_subtasks_list and sources_metrics_list on Timestamp index
        operator_metrics_df = pd.concat(operator_metrics_list, axis=1)

        sources_metrics_df = pd.concat(sources_metrics_list, axis=1)

        job_metrics_df = pd.concat(job_metrics_list, axis=1)

        final_df = pd.concat([operator_metrics_df, sources_metrics_df], axis=1)

        # Start time from 0
        final_df.index = (final_df.index - final_df.index.min()) / 1000

        # Set index as int
        final_df.index = final_df.index.astype(int)

        # Add df from job metrics to final_df
        state_metrics_df = pd.concat(state_metrics_list, axis=1)
        final_df = pd.concat([final_df, state_metrics_df], axis=1)

        # Add Parallelism column
        numRecordsInPerSecond_cols = [
            col
            for col in final_df.columns
            if any("numRecordsInPerSecond" in str(item) for item in col)
        ]
        final_df["Parallelism"] = final_df[numRecordsInPerSecond_cols].count(axis=1)

        # self.final_df = final_df
        final_df.to_csv(os.path.join(self.exp_path, "final_df.csv"))

        job_metrics_df.to_csv(os.path.join(self.exp_path, "job_metrics_df.csv"))
