from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from scripts.src.data.exporting.exporter import Exporter
from scripts.src.data.exporting.strategies.csv_export_strategy import CsvExportStrategy
from scripts.src.data.loading.loader import Loader
from scripts.src.data.loading.strategies.file_load_strategy import FileLoadStrategy
from scripts.src.data.plotting.default_plotter import DefaultPlotter
from scripts.utils.Logger import Logger


class BaseProcessingStrategy(ABC):
    """Base class for different grouped experiment processing strategies."""

    def __init__(self, logger: Logger, exp_path: Path):
        self.logger = logger
        self.exp_path = exp_path
        self.loader = Loader(FileLoadStrategy(self.logger))
        self.exporter = Exporter(CsvExportStrategy(self.logger))
        plots_path = self.exp_path / "plots"
        plots_path.mkdir(exist_ok=True)
        self.plotter = DefaultPlotter(self.logger, str(plots_path))

    @abstractmethod
    def process(self) -> Dict[str, Any]:
        """Executes the specific processing and plotting logic."""
        pass
