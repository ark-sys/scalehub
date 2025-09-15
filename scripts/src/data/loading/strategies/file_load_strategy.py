from pathlib import Path
from typing import Dict

import pandas as pd

from scripts.src.data.loading.strategies.base_load_strategy import BaseLoadStrategy
from scripts.utils.Logger import Logger


class FileLoadStrategy(BaseLoadStrategy):
    """Strategy for loading data from a local file."""

    def load(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Loads data from a specified file path.

        :param kwargs: Expects 'file_path' (str or Path).
        :return: A dictionary containing a single DataFrame keyed by the file stem.
        """
        file_path = kwargs.get("file_path")
        if not file_path:
            self._logger.error("File path not provided for FileLoadStrategy.")
            raise ValueError("'file_path' is required for FileLoadStrategy")

        path = Path(file_path)
        if not path.exists():
            self._logger.error(f"File not found at {path}")
            raise FileNotFoundError(f"File not found at {path}")

        try:
            self._logger.info(f"Loading data from {path}...")
            df = pd.read_csv(path)
            return {path.stem: df}
        except Exception as e:
            self._logger.error(f"Failed to load data from {path}: {e}")
            raise
