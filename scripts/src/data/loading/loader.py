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
