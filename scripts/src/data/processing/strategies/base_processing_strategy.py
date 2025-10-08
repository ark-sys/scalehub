from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from scripts.src.data.processing.base_processor import ProcessorWithComponents


class BaseProcessingStrategy(ProcessorWithComponents, ABC):
    """Base class for different grouped experiment processing strategies.

    Extends ProcessorWithComponents to inherit common loader, exporter, and plotter setup.
    """

    def __init__(self, logger, exp_path: Path):
        # Convert Path to str for parent constructor
        super().__init__(logger, str(exp_path))

    @abstractmethod
    def process(self) -> Dict[str, Any]:
        """Executes the specific processing and plotting logic."""
        pass
