from pathlib import Path
from typing import Dict, Any

from scripts.src.data.base.plotter import PlotterInterface
from scripts.src.data.factories.plot_strategy_factory import PlotStrategyFactory


class DefaultPlotter(PlotterInterface):
    """Default implementation of the PlotterInterface using strategies."""

    def __init__(self, logger, plots_path: str, **style_params):
        super().__init__(logger, plots_path)
        self.style_params = style_params
        self.factory = PlotStrategyFactory()

    def generate_plot(
        self, data: Dict[str, Any], plot_type: str = "basic", **kwargs
    ) -> Path:
        """Generate a plot using the appropriate strategy."""
        try:
            strategy = self.factory.create_strategy(plot_type)

            # Initialize strategy with style parameters
            strategy_instance = strategy(
                self._logger, self._plots_path, **self.style_params
            )

            return strategy_instance.generate(data, **kwargs)
        except Exception as e:
            self._logger.error(f"Failed to generate plot of type '{plot_type}': {e}")
            raise
