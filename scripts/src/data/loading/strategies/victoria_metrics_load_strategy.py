import pandas as pd
import requests

from scripts.src.data.loading.strategies.base_load_strategy import BaseLoadStrategy
from scripts.utils.Logger import Logger


class VictoriaMetricsLoadStrategy(BaseLoadStrategy):
    """Strategy for loading data from VictoriaMetrics."""

    def __init__(self, logger: Logger, db_url: str, start_ts: str, end_ts: str):
        super().__init__(logger)
        self.db_url = db_url
        self.db_url_local = "localhost:8428"
        self.start_ts = start_ts
        self.end_ts = end_ts

    def load(self, **kwargs) -> dict[str, pd.DataFrame]:
        """Load data from VictoriaMetrics."""
        self._logger.info("Loading from VictoriaMetrics...")
        exported_data = {}
        time_series_to_load = kwargs.get(
            "time_series",
            [
                "flink_taskmanager_job_task_numRecordsInPerSecond",
                "flink_taskmanager_job_task_busyTimeMsPerSecond",
                "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond",
            ],
        )

        for ts_name in time_series_to_load:
            try:
                _, df = self._load_timeseries_as_df(ts_name)
                if df is not None:
                    exported_data[ts_name] = df
                    self._logger.info(f"Successfully loaded {ts_name}")
            except Exception as e:
                self._logger.error(f"Failed to load {ts_name}: {e}")
        return exported_data

    def _fetch_data(self, url, params):
        """Fetch data from VictoriaMetrics with fallback to local."""
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            self._logger.warning(f"Failed to connect to {self.db_url}. Trying local.")
            url = f"http://{self.db_url_local}/api/v1/export/csv"
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response

    def _load_timeseries_as_df(self, time_series_name: str):
        """Load time series and return it as a DataFrame."""
        url = f"http://{self.db_url}/api/v1/export/csv"
        params = {
            "format": "__name__,__timestamp__:unix_s,__value__",
            "match[]": time_series_name,
            "start": self.start_ts,
            "end": self.end_ts,
        }
        response = self._fetch_data(url, params)

        if response.status_code == 200:
            # Use a temporary in-memory buffer instead of writing a file
            from io import StringIO

            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)
            df.columns = ["Series", "Timestamp", "Value"]
            df = df[["Timestamp", "Value"]]
            df.set_index("Timestamp", inplace=True)
            df.sort_index(inplace=True)
            df.index = df.index - df.index.min()
            return time_series_name, df
        else:
            self._logger.error(f"Error loading data: {response.text}")
            return None, None
