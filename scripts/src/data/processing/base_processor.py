from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from scripts.utils.Logger import Logger


class DataProcessor(ABC):
    """Abstract base class for data processing."""

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
        """Main processing method for the experiment."""
        pass

    @property
    def exp_path(self) -> Path:
        """Get experiment path."""
        return self._exp_path

    @property
    def logger(self) -> Logger:
        """Get logger instance."""
        return self._logger
