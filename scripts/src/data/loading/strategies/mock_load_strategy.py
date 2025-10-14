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

import numpy as np
import pandas as pd

from scripts.src.data.loading.strategies.base_load_strategy import BaseLoadStrategy


class MockLoadStrategy(BaseLoadStrategy):
    """Mock strategy for loading sample data for testing purposes."""

    def load(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Generates a mock DataFrame.

        :param kwargs: Not used, but present for compatibility.
        :return: A dictionary containing a single mock DataFrame.
        """
        self._logger.info("Loading data using MockLoadStrategy...")
        data = {
            "Timestamp": pd.to_datetime(np.arange(10), unit="s"),
            "Value": np.random.rand(10) * 100,
        }
        df = pd.DataFrame(data).set_index("Timestamp")
        return {"mock_data": df}
