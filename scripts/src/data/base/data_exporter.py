from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

import pandas as pd

from scripts.utils.Logger import Logger


class DataExporter(ABC):
    """Abstract base class for data exporters."""

    def __init__(self, logger: Logger, exp_path: str):
        self._logger = logger
        self._exp_path = Path(exp_path)
        self._export_path = self._exp_path / "export"
        self._export_path.mkdir(exist_ok=True)

    @abstractmethod
    def export(self) -> Dict[str, pd.DataFrame]:
        """Export data from source."""
        pass

    @property
    def exp_path(self) -> Path:
        return self._exp_path

    @property
    def export_path(self) -> Path:
        return self._export_path

    @property
    def logger(self) -> Logger:
        return self._logger
