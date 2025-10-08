from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from scripts.src.data.exporting.exporter import Exporter
from scripts.src.data.exporting.strategies.csv_export_strategy import CsvExportStrategy
from scripts.src.data.loading.loader import Loader
from scripts.src.data.loading.strategies.file_load_strategy import FileLoadStrategy
from scripts.src.data.plotting.default_plotter import DefaultPlotter
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


class ProcessorWithComponents(DataProcessor, ABC):
    """Base processor with loader, exporter, and plotter components.

    This class provides common setup for all processors that need to load data,
    export results, and generate plots.
    """

    def __init__(self, logger: Logger, exp_path: str):
        super().__init__(logger, exp_path)
        self._setup_components()

    def _setup_components(self) -> None:
        """Initialize required components (loader, exporter, plotter)."""
        self.loader = Loader(FileLoadStrategy(self.logger))
        self.exporter = Exporter(CsvExportStrategy(self.logger))

        plots_path = self.exp_path / "plots"
        plots_path.mkdir(exist_ok=True)
        self.plotter = DefaultPlotter(self.logger, str(plots_path))
