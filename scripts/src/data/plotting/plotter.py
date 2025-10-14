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
