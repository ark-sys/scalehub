from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any


class PlotStrategy(ABC):
    @abstractmethod
    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate specific type of plot."""
        pass
