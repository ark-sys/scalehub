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

from pathlib import Path

import pandas as pd

from scripts.src.data.exporting.strategies.base_export_strategy import BaseExportStrategy


class Exporter:
    """Context class for exporting data."""

    def __init__(self, strategy: BaseExportStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: BaseExportStrategy) -> None:
        """Sets a new export strategy."""
        self._strategy = strategy

    def export_data(self, data: pd.DataFrame, output_path: Path) -> None:
        """
        Executes the export using the current strategy.

        :param data: The pandas DataFrame to export.
        :param output_path: The destination file path.
        """
        self._strategy.export(data, output_path)
