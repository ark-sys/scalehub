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
