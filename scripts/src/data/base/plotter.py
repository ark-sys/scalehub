from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from scripts.utils.Logger import Logger


class PlotterInterface(ABC):
    """Abstract interface for plotting components."""

    def __init__(self, logger: Logger, plots_path: str):
        self._logger = logger
        self._plots_path = Path(plots_path)
        self._plots_path.mkdir(exist_ok=True)

    @abstractmethod
    def generate_plot(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a plot from data."""
        pass

    @property
    def plots_path(self) -> Path:
        return self._plots_path

    @property
    def logger(self) -> Logger:
        return self._logger
