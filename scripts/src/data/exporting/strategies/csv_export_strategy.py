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


class CsvExportStrategy(BaseExportStrategy):
    """Strategy for exporting data to a CSV file."""

    def export(self, data: pd.DataFrame, output_path: Path) -> None:
        """Exports the DataFrame to a CSV file."""
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(output_path)
            self._logger.info(f"Data successfully exported to {output_path}")
        except Exception as e:
            self._logger.error(f"Failed to export data to {output_path}: {e}")
            raise
