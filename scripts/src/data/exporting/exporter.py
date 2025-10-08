from pathlib import Path

import pandas as pd

from scripts.src.data.exporting.strategies.base_export_strategy import BaseExportStrategy


class Exporter:
    """Context class for exporting data."""

    def __init__(self, strategy: BaseExportStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: BaseExportStrategy) -> None:
        """Sets a new export strategy."""
        self._strategy = strategy

    def export_data(self, data: pd.DataFrame, output_path: Path) -> None:
        """
        Executes the export using the current strategy.

        :param data: The pandas DataFrame to export.
        :param output_path: The destination file path.
        """
        self._strategy.export(data, output_path)
