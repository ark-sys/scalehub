from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from scripts.utils.Logger import Logger


class DataProcessor(ABC):
    """Abstract base class for data processors following Template Method pattern."""

    def __init__(self, logger: Logger, exp_path: str):
        self._logger = logger
        self._exp_path = Path(exp_path)
        self._validate_path()

    def _validate_path(self) -> None:
        """Validate that the experiment path exists."""
        if not self._exp_path.exists():
            raise FileNotFoundError(f"Experiment path does not exist: {self._exp_path}")

    @abstractmethod
    def process(self) -> Dict[str, Any]:
        """Main processing method - Template Method."""
        pass

    @abstractmethod
    def _load_data(self) -> pd.DataFrame:
        """Load raw data - to be implemented by subclasses."""
        pass

    @abstractmethod
    def _transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform loaded data - to be implemented by subclasses."""
        pass

    def _save_results(self, data: pd.DataFrame, filename: str) -> Path:
        """Save processed data to file."""
        output_path = self._exp_path / filename
        data.to_csv(output_path)
        self._logger.info(f"Results saved to: {output_path}")
        return output_path

    @property
    def exp_path(self) -> Path:
        """Get experiment path."""
        return self._exp_path

    @property
    def logger(self) -> Logger:
        """Get logger instance."""
        return self._logger
