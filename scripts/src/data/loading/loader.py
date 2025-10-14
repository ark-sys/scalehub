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

from typing import Dict

import pandas as pd

from scripts.src.data.loading.strategies.base_load_strategy import BaseLoadStrategy


class Loader:
    """Context class for loading data."""

    def __init__(self, strategy: BaseLoadStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: BaseLoadStrategy) -> None:
        """Sets a new loading strategy."""
        self._strategy = strategy

    def load_data(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Executes the load using the current strategy.

        :param kwargs: Arguments to be passed to the strategy's load method.
        :return: A dictionary of pandas DataFrames.
        """
        return self._strategy.load(**kwargs)
