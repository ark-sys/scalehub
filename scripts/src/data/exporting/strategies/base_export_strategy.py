from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from scripts.utils.Logger import Logger


class BaseExportStrategy(ABC):
    """Abstract base class for data exporting (saving) strategies."""

    def __init__(self, logger: Logger):
        self._logger = logger

    @abstractmethod
    def export(self, data: pd.DataFrame, output_path: Path) -> None:
        """
        Exports the given DataFrame to the specified path.

        :param data: The pandas DataFrame to export.
        :param output_path: The destination file path.
        """
        pass
