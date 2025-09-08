import json
from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd
import requests

from scripts.utils.Logger import Logger


class ExportStrategy(ABC):
    """Abstract strategy for different export methods."""

    def __init__(self, logger: Logger):
        self._logger = logger

    @abstractmethod
    def export(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """Execute export strategy."""
        pass


class VictoriaMetricsExportStrategy(ExportStrategy):
    """Strategy for exporting from VictoriaMetrics."""

    def __init__(self, logger: Logger, db_url: str, start_ts: str, end_ts: str):
        super().__init__(logger)
        self.db_url = db_url
        self.db_url_local = "localhost:8428"
        self.start_ts = start_ts
        self.end_ts = end_ts

    def export(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """Export data from VictoriaMetrics."""
        self._logger.info("Exporting from VictoriaMetrics...")

        # Export common time series
        exported_data = {}

        time_series_to_export = [
            "flink_taskmanager_job_task_numRecordsInPerSecond",
            "flink_taskmanager_job_task_busyTimeMsPerSecond",
            "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond",
        ]

        for ts_name in time_series_to_export:
            try:
                output_file, df = self.export_timeseries_csv(ts_name)
                if df is not None:
                    exported_data[ts_name] = df
                    self._logger.info(f"Successfully exported {ts_name}")
            except Exception as e:
                self._logger.error(f"Failed to export {ts_name}: {e}")

        return exported_data

    def fetch_data(self, url, params):
        """Fetch data from VictoriaMetrics with fallback to local."""
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self._logger.warning(
                f"Failed to connect to VictoriaMetrics at {self.db_url}: {str(e)}"
            )
            self._logger.info(
                f"Trying to connect to local VictoriaMetrics at {self.db_url_local}"
            )
            url = f"http://{self.db_url_local}/api/v1/export"
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response

    def export_timeseries_csv(
        self,
        time_series_name: str,
        format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        """Export time series as CSV."""
        output_file = f"{time_series_name}_export.csv"

        url = f"http://{self.db_url}/api/v1/export/csv"
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

            # Process the CSV
            df = pd.read_csv(output_file, index_col="Timestamp")
            df = df.drop(df.columns[0], axis=1)
            df.columns = ["Timestamp", time_series_name]
            df.set_index("Timestamp", inplace=True)
            df.sort_index(inplace=True)
            df.index = df.index - df.index.min()
            df.to_csv(output_file)

            self._logger.info(f"Data exported to {output_file}")
            return output_file, df
        else:
            self._logger.error(f"Error exporting data: {response.text}")
            return None, None


class FileExportStrategy(ExportStrategy):
    """Strategy for exporting from local files."""

    def __init__(self, logger: Logger, file_path: str):
        super().__init__(logger)
        self.file_path = file_path

    def export(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """Export data from local files."""
        self._logger.info(f"Exporting from local files at {self.file_path}")

        try:
            if self.file_path.endswith(".csv"):
                df = pd.read_csv(self.file_path)
                return {"file_data": df}
            elif self.file_path.endswith(".json"):
                with open(self.file_path, "r") as f:
                    data = json.load(f)
                return {"json_data": pd.DataFrame(data)}
            else:
                self._logger.error(f"Unsupported file format: {self.file_path}")
                return {}
        except Exception as e:
            self._logger.error(f"Error reading file {self.file_path}: {e}")
            return {}


class MockExportStrategy(ExportStrategy):
    """Mock strategy for testing purposes."""

    def export(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """Mock export for testing."""
        self._logger.info("Using mock export strategy")

        # Generate mock data
        mock_data = pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=100, freq="1min"),
                "throughput": range(100),
                "latency": [i * 0.1 for i in range(100)],
            }
        )

        return {"mock_data": mock_data}
