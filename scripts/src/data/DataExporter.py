import json
from typing import Dict

import pandas as pd

from scripts.src.data.base.data_exporter import DataExporter as BaseDataExporter
from scripts.src.data.strategies.export_strategy import VictoriaMetricsExportStrategy
from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class DataExporter(BaseDataExporter):
    """Data exporter using strategy pattern for different export methods."""

    def __init__(self, log: Logger, exp_path: str, force: bool = False):
        super().__init__(log, exp_path)
        self.force = force
        self._setup_configuration()
        self._setup_strategy()

    def _setup_configuration(self) -> None:
        """Setup configuration from experiment log."""
        log_file = self.exp_path / "exp_log.json"
        self.config = Config(self.logger, str(log_file))

        try:
            with open(log_file, "r") as f:
                logs = json.load(f)
                self.start_ts = logs["timestamps"]["start"]
                self.end_ts = logs["timestamps"]["end"]
        except Exception as e:
            self.logger.error(f"Failed to load json file {log_file}: {e}")
            raise

    def _setup_strategy(self) -> None:
        """Setup appropriate export strategy."""
        db_url = "victoria-metrics-single-server.default.svc.cluster.local:8428"
        self._export_strategy = VictoriaMetricsExportStrategy(
            self.logger, db_url, self.start_ts, self.end_ts
        )

    def export(self) -> Dict[str, pd.DataFrame]:
        """Export data using configured strategy."""
        try:
            self.logger.info("Starting data export...")
            results = self._export_strategy.export()
            self.logger.info("Data export completed successfully")
            return results
        except Exception as e:
            self.logger.error(f"[DATA_EXP] Error exporting data: {e}")
            raise

    def load_json(self, file_path: str) -> list[dict]:
        """Utility method for loading JSON files."""
        try:
            with open(file_path, "r") as file:
                return [json.loads(line) for line in file]
        except Exception as e:
            self.logger.warning(
                f"Failed to load json file {file_path} due to : {str(e)}\nTrying fix..."
            )
            try:
                with open(file_path, "r") as file:
                    content = file.read().replace("\n", "").replace("}{", "}\n{")
                    return [json.loads(line) for line in content.split("\n")]
            except Exception as e:
                self.logger.error(
                    f"Failed to load json file {file_path} due to : {str(e)}"
                )
                raise e

    def change_strategy(self, strategy_type: str, **kwargs) -> None:
        """Change the export strategy dynamically."""
        if strategy_type == "victoria_metrics":
            db_url = kwargs.get(
                "db_url",
                "victoria-metrics-single-server.default.svc.cluster.local:8428",
            )
            self._export_strategy = VictoriaMetricsExportStrategy(
                self.logger, db_url, self.start_ts, self.end_ts
            )
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")

    @property
    def export_strategy(self):
        """Get current export strategy."""
        return self._export_strategy
