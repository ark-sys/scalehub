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
from typing import Dict

import pandas as pd

from src.scalehub.data.loading.strategies.base_load_strategy import BaseLoadStrategy


class FileLoadStrategy(BaseLoadStrategy):
    """Strategy for loading data from a local file."""

    def load(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Loads data from a specified file path.

        :param kwargs: Expects 'file_path' (str or Path).
        :return: A dictionary containing a single DataFrame keyed by the file stem.
        """
        file_path = kwargs.get("file_path")
        if not file_path:
            self._logger.error("File path not provided for FileLoadStrategy.")
            raise ValueError("'file_path' is required for FileLoadStrategy")

        path = Path(file_path)
        if not path.exists():
            self._logger.error(f"File not found at {path}")
            raise FileNotFoundError(f"File not found at {path}")

        try:
            self._logger.info(f"Loading data from {path}...")
            df = pd.read_csv(path)
            return {path.stem: df}
        except Exception as e:
            self._logger.error(f"Failed to load data from {path}: {e}")
            raise
