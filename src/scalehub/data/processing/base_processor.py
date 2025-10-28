# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from src.scalehub.data.exporting.exporter import Exporter
from src.scalehub.data.exporting.strategies.csv_export_strategy import CsvExportStrategy
from src.scalehub.data.loading.loader import Loader
from src.scalehub.data.loading.strategies.file_load_strategy import FileLoadStrategy
from src.scalehub.data.plotting.default_plotter import DefaultPlotter
from src.utils.Logger import Logger


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
