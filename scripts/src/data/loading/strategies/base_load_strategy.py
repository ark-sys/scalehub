from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd

from scripts.utils.Logger import Logger


class BaseLoadStrategy(ABC):
    """Abstract strategy for different data loading methods."""

    def __init__(self, logger: Logger):
        self._logger = logger

    @abstractmethod
    def load(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """Execute loading strategy and return a dictionary of DataFrames."""
        pass
