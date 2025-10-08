from pathlib import Path

import pandas as pd

from scripts.src.data.exporting.strategies.base_export_strategy import BaseExportStrategy


class CsvExportStrategy(BaseExportStrategy):
    """Strategy for exporting data to a CSV file."""

    def export(self, data: pd.DataFrame, output_path: Path) -> None:
        """Exports the DataFrame to a CSV file."""
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(output_path)
            self._logger.info(f"Data successfully exported to {output_path}")
        except Exception as e:
            self._logger.error(f"Failed to export data to {output_path}: {e}")
            raise
