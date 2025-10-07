import json

import pandas as pd
import requests

from scripts.src.data.loading.strategies.base_load_strategy import BaseLoadStrategy
from scripts.utils.Logger import Logger


class VictoriaMetricsLoadStrategy(BaseLoadStrategy):
    """Strategy for loading data from VictoriaMetrics."""

    def __init__(self, logger: Logger, db_url: str, start_ts: str, end_ts: str):
        super().__init__(logger)
        self.db_url = db_url
        # Define fallback URLs to try in order
        self.fallback_urls = [
            # "vm.scalehub.dev",
            "127.0.0.1:8428",
        ]
        self.start_ts = start_ts
        self.end_ts = end_ts

    def load(self, **kwargs) -> dict[str, pd.DataFrame]:
        """Load data from VictoriaMetrics."""
        self._logger.info("Loading from VictoriaMetrics...")
        exported_data = {}
        time_series_to_load = kwargs.get(
            "time_series",
            [
                # Core Flink metrics
                "flink_taskmanager_job_task_numRecordsInPerSecond",
                "flink_taskmanager_job_task_busyTimeMsPerSecond",
                "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond",
                # Checkpointing metrics
                "flink_jobmanager_job_lastCheckpointDuration",
                "flink_taskmanager_job_task_checkpointStartDelayNanos",
                # Operator metrics
                "flink_taskmanager_job_task_operator_numLateRecordsDropped",
                # JVM metrics
                "flink_taskmanager_Status_JVM_CPU_Load",
                "flink_taskmanager_Status_JVM_CPU_Time",
                "flink_taskmanager_Status_JVM_Threads_Count",
                "flink_taskmanager_Status_JVM_Memory_Heap_Used",
                "flink_taskmanager_Status_Flink_Memory_Managed_Used",
                "flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time",
            ],
        )

        export_format = kwargs.get("format", "csv")  # csv or json

        for ts_name in time_series_to_load:
            try:
                if export_format == "json":
                    json_data = self._load_timeseries_as_json(ts_name)
                    if json_data:
                        exported_data[ts_name] = json_data
                        self._logger.info(f"Successfully loaded {ts_name} as JSON")
                else:
                    _, df = self._load_timeseries_as_df(ts_name)
                    if df is not None:
                        exported_data[ts_name] = df
                        self._logger.info(f"Successfully loaded {ts_name} as CSV")
            except Exception as e:
                self._logger.error(f"Failed to load {ts_name}: {e}")
        return exported_data

    def _fetch_data(self, url, params, is_json=False):
        """Fetch data from VictoriaMetrics with fallback to alternative URLs."""
        # Try the primary URL first
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self._logger.warning(f"Failed to connect to {self.db_url}: {e}")

            # Try fallback URLs
            for fallback_url in self.fallback_urls:
                try:
                    self._logger.info(f"Trying fallback URL: {fallback_url}")
                    # Use correct endpoint based on format
                    if is_json:
                        fallback_full_url = f"http://{fallback_url}/api/v1/export"
                    else:
                        fallback_full_url = f"http://{fallback_url}/api/v1/export/csv"
                    response = requests.get(fallback_full_url, params=params, timeout=5)
                    response.raise_for_status()
                    self._logger.info(f"Successfully connected to {fallback_url}")
                    return response
                except requests.exceptions.RequestException as fallback_e:
                    self._logger.warning(
                        f"Failed to connect to {fallback_url}: {fallback_e}"
                    )
                    continue

            # If all attempts failed, raise the original exception
            raise e

    def _load_timeseries_as_df(self, time_series_name: str):
        """Load time series and return it as a DataFrame."""
        url = f"http://{self.db_url}/api/v1/export/csv"
        params = {
            "format": "__name__,__timestamp__:unix_s,__value__",
            "match[]": time_series_name,
            "start": self.start_ts,
            "end": self.end_ts,
        }
        response = self._fetch_data(url, params, is_json=False)

        if response.status_code == 200:
            # Check if response has data
            if not response.text or response.text.strip() == "":
                self._logger.warning(f"No data returned for {time_series_name}")
                return None, None

            # Use a temporary in-memory buffer instead of writing a file
            from io import StringIO

            csv_data = StringIO(response.text)
            try:
                df = pd.read_csv(csv_data)
                if df.empty:
                    self._logger.warning(f"Empty dataframe for {time_series_name}")
                    return None, None

                df.columns = ["Series", "Timestamp", "Value"]
                df = df[["Timestamp", "Value"]]
                df.set_index("Timestamp", inplace=True)
                df.sort_index(inplace=True)
                df.index = df.index - df.index.min()
                return time_series_name, df
            except pd.errors.EmptyDataError:
                self._logger.warning(f"No data to parse for {time_series_name}")
                return None, None
        else:
            self._logger.error(f"Error loading data: {response.text}")
            return None, None

    def _load_timeseries_as_json(self, time_series_name: str):
        """Load time series and return it as JSON data."""
        url = f"http://{self.db_url}/api/v1/export"
        params = {
            "match[]": time_series_name,
            "start": self.start_ts,
            "end": self.end_ts,
        }
        response = self._fetch_data(url, params, is_json=True)

        if response.status_code == 200:
            # Check if response has data
            if not response.text or response.text.strip() == "":
                self._logger.warning(f"No data returned for {time_series_name}")
                return None

            try:
                # Parse JSON lines (each line is a separate JSON object)
                json_data = [
                    json.loads(line)
                    for line in response.text.strip().split("\n")
                    if line.strip()
                ]
                if not json_data:
                    self._logger.warning(f"Empty JSON data for {time_series_name}")
                    return None
                return json_data
            except json.JSONDecodeError as e:
                self._logger.error(f"Failed to parse JSON for {time_series_name}: {e}")
                return None
        else:
            self._logger.error(f"Error loading data: {response.text}")
            return None
