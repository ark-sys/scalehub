import json
import os

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
        self.exp_path = exp_path
        self.export_path = os.path.join(self.exp_path, "export")
        os.makedirs(self.export_path, exist_ok=True)

        log_file = os.path.join(self.exp_path, "exp_log.json")
        self.config = Config(log, log_file)

        try:
            with open(log_file, "r") as log_file:
                logs = json.load(log_file)
                self.start_ts = logs["timestamps"]["start"]
                self.end_ts = logs["timestamps"]["end"]
        except Exception as e:
            self.__log.error(f"Failed to load json file {log_file} due to : {str(e)}")
            raise e

        self.db_url = "victoria-metrics-single-server.default.svc.cluster.local:8428"
        self.db_url_local = "localhost/vm"

    def load_json(self, file_path) -> [dict]:
        try:
            with open(file_path, "r") as file:
                return [json.loads(line) for line in file]
        except Exception as e:
            self.__log.warning(
                f"Failed to load json file {file_path} due to : {str(e)}\nTrying fix..."
            )
            try:
                with open(file_path, "r") as file:
                    content = file.read().replace("\n", "").replace("}{", "}\n{")
                    return [json.loads(line) for line in content.split("\n")]
            except Exception as e:
                self.__log.error(
                    f"Failed to load json file {file_path} due to : {str(e)}"
                )
                raise e

    def fetch_data(self, url, params):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.__log.warning(
                f"Failed to connect to VictoriaMetrics at {self.db_url}: {str(e)}"
            )
            self.__log.info(
                f"Trying to connect to local VictoriaMetrics at {self.db_url_local}"
            )
            url = f"http://{self.db_url_local}/api/v1/export"
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response

    def export_timeseries(
        self,
        time_series_name: str,
        format_labels="__name__,__timestamp__:unix_s,__value__",
        file_type="json",
    ):
        output_file = os.path.join(
            self.export_path, f"{time_series_name}_export.{file_type}"
        )
        if os.path.exists(output_file) and not self.force:
            self.__log.info(
                f"Skipping export of {time_series_name} timeseries: file exists."
            )
            return output_file

        url = (
            f"http://{self.db_url}/api/v1/export"
            if file_type == "json"
            else f"http://{self.db_url}/api/v1/export/csv"
        )
        params = {
            "format": format_labels,
            "match[]": time_series_name,
            "start": self.start_ts,
            "end": self.end_ts,
        }
        response = self.fetch_data(url, params)

        if response.status_code == 200:
            with open(output_file, "wb") as file:
                file.write(response.content)
            self.__log.info(f"Data exported to {output_file}")
            return output_file
        else:
            self.__log.error(f"Error exporting data: {response.text}")

    def export_timeseries_json(
        self,
        time_series_name: str,
        format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        return self.export_timeseries(time_series_name, format_labels, "json")

    def export_timeseries_csv(
        self,
        time_series_name: str,
        format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        output_file = self.export_timeseries(time_series_name, format_labels, "csv")
        if output_file:
            df = pd.read_csv(output_file, index_col="Timestamp")
            df = df.drop(df.columns[0], axis=1)
            df.columns = ["Timestamp", time_series_name]
            df.set_index("Timestamp", inplace=True)
            df.sort_index(inplace=True)
            df.index = df.index - df.index.min()
            df.to_csv(output_file)
            return output_file, df

    def perf_query(self, query: str):
        query_url = f"http://{self.db_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": self.start_ts,
            "end": self.end_ts,
            "step": "10s",
        }
        self.__log.info(f"Performing query with parameters: {params}")
        response = self.fetch_data(query_url, params)
        if response.status_code == 200:
            return response.json()
        else:
            self.__log.error(f"Error querying data: {response.text}")

    @staticmethod
    def process_metrics(metrics_content, task_name):
        data = {}
        for metric in metrics_content:
            if metric["metric"]["task_name"] == task_name:
                subtask_index = metric["metric"]["subtask_index"]
                if subtask_index not in data:
                    data[subtask_index] = []
                for value, timestamp in zip(metric["values"], metric["timestamps"]):
                    data[subtask_index].append((round(timestamp / 5000), value))
        return data

    def get_metrics_per_subtask(
        self, metrics_content, metric_name, task_name
    ) -> tuple[str, pd.DataFrame]:
        data = self.process_metrics(metrics_content, task_name)
        output_file = os.path.join(self.export_path, f"{metric_name}_export.csv")
        df = pd.DataFrame(
            {
                f"{metric_name}_{k}": pd.Series(dict(v), name=f"{metric_name}_{k}")
                for k, v in data.items()
            }
        )
        df.sort_index(inplace=True)
        df.columns = (
            df.columns.astype(str).str.extract(r"(\d+)", expand=False).astype(int)
        )
        df = df.sort_index(axis=1)
        df.columns = pd.MultiIndex.from_product(
            [[metric_name], [task_name], df.columns]
        )
        df.index = df.index * 5000
        df.index.name = "Timestamp"
        df.to_csv(output_file)
        return output_file, df

    def get_sources_metrics(self, metrics_content, metric_name) -> [pd.DataFrame]:
        sources = {
            metric["metric"]["task_name"]
            for metric in metrics_content
            if "Source" in metric["metric"]["task_name"]
        }
        res = []
        for source in sources:
            data = self.process_metrics(metrics_content, source)
            output_file = os.path.join(
                self.export_path, f"{metric_name}_{source}_export.csv"
            )
            df = pd.DataFrame(
                {
                    f"{metric_name}_{source}_{k}": pd.Series(
                        dict(v), name=f"{metric_name}_{k}"
                    )
                    for k, v in data.items()
                }
            )
            df.sort_index(inplace=True)
            df.columns = df.columns.str.extract(r"(\d+)", expand=False).astype(int)
            df = df.sort_index(axis=1)
            df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])
            df.index = df.index * 5000
            df.index.name = "Timestamp"
            res.append(df)
            df.to_csv(output_file)
        return res

    def export(self):
        try:
            job_name = self.config.get(Key.Experiment.name.key)
            tasks = (
                list(MAP_PIPELINE_DICT.values())
                if job_name == "Map"
                else list(JOIN_PIPELINE_DICT.values())
            )
            if job_name == "Join":
                tasks = [
                    task
                    for sublist in tasks
                    for task in (sublist if isinstance(sublist, tuple) else [sublist])
                ]
            else:
                self.__log.error(f"Unknown job name: {job_name}")
                exit(1)

            operator_name = self.config.get(Key.Experiment.task_name.key)
            if operator_name == "TumblingEventTimeWindows":
                operator_name = "TumblingEventTimeWindows____Timestamps_Watermarks"

            operator_metrics_list = [
                self.get_metrics_per_subtask(
                    self.load_json(self.export_timeseries_json(metric)),
                    metric,
                    operator_name,
                )[1]
                for metric in metrics_dict["operator_metrics"]
            ]
            sources_metrics_list = [
                df
                for metric in metrics_dict["sources_metrics"]
                for df in self.get_sources_metrics(
                    self.load_json(self.export_timeseries_json(metric)), metric
                )
            ]
            # state_metrics_list = [
            #     self.export_timeseries_csv(metric)[1]
            #     for metric in metrics_dict["state_metrics"]
            # ]
            job_metrics_list = [
                self.get_metrics_per_subtask(
                    self.load_json(self.export_timeseries_json(metric)),
                    metric,
                    task_name,
                )[1]
                for metric in metrics_dict["job_metrics"]
                for task_name in tasks
            ]

            operator_metrics_df = pd.concat(operator_metrics_list, axis=1)
            sources_metrics_df = pd.concat(sources_metrics_list, axis=1)
            job_metrics_df = pd.concat(job_metrics_list, axis=1)
            final_df = pd.concat([operator_metrics_df, sources_metrics_df], axis=1)
            final_df.index = (final_df.index - final_df.index.min()) / 1000
            final_df.index = final_df.index.astype(int)
            # state_metrics_df = pd.concat(state_metrics_list, axis=1)
            # final_df = pd.concat([final_df, state_metrics_df], axis=1)
            final_df["Parallelism"] = final_df[
                [col for col in final_df.columns if "numRecordsInPerSecond" in str(col)]
            ].count(axis=1)

            final_df.to_csv(os.path.join(self.exp_path, "final_df.csv"))
            job_metrics_df.to_csv(os.path.join(self.exp_path, "job_metrics_df.csv"))

        except Exception as e:
            self.__log.error(f"[DATA_EXP] Error exporting data: {e}")
            raise e
