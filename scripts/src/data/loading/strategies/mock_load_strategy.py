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
