from scripts.src.data.strategies.basic_plot_strategy import BasicPlotStrategy
from scripts.src.data.strategies.single_frame_plot_strategy import (
    SingleFramePlotStrategy,
)
from scripts.src.data.strategies.stacked_frames_plot_strategy import (
    StackedFramesPlotStrategy,
)
from scripts.src.data.strategies.stacked_plot_strategy import StackedPlotStrategy
from scripts.src.data.strategies.three_d_plot_strategy import ThreeDPlotStrategy
from scripts.src.data.strategies.whisker_plot_strategy import WhiskerPlotStrategy


class PlotStrategyFactory:
    """Factory for creating plot strategy instances."""

    _strategies = {
        "basic": BasicPlotStrategy,
        "stacked": StackedPlotStrategy,
        "single_frame": SingleFramePlotStrategy,
        "whisker": WhiskerPlotStrategy,
        "3d": ThreeDPlotStrategy,
        "stacked_frames": StackedFramesPlotStrategy,
    }

    @classmethod
    def create_strategy(cls, plot_type: str):
        """Create and return a strategy class for the given plot type."""
        if plot_type not in cls._strategies:
            raise ValueError(
                f"Unknown plot type: {plot_type}. Available types: {list(cls._strategies.keys())}"
            )

        return cls._strategies[plot_type]

    @classmethod
    def register_strategy(cls, plot_type: str, strategy_class):
        """Register a new strategy type."""
        cls._strategies[plot_type] = strategy_class

    @classmethod
    def available_strategies(cls):
        """Return list of available strategy types."""
        return list(cls._strategies.keys())
