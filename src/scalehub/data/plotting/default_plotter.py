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
from typing import Dict, Any

from src.scalehub.data.plotting.factory import PlotStrategyFactory
from src.scalehub.data.plotting.plotter import PlotterInterface


class DefaultPlotter(PlotterInterface):
    """Default implementation of the PlotterInterface using strategies."""

    def __init__(self, logger, plots_path: str, **style_params):
        super().__init__(logger, plots_path)
        self.style_params = style_params
        self.factory = PlotStrategyFactory()

    def generate_plot(self, data: Dict[str, Any], plot_type: str = "basic", **kwargs) -> Path:
        """Generate a plot using the appropriate strategy."""
        try:
            strategy = self.factory.create_strategy(plot_type)

            # Initialize strategy with style parameters
            strategy_instance = strategy(self._logger, self._plots_path, **self.style_params)

            return strategy_instance.generate(data, **kwargs)
        except Exception as e:
            self._logger.error(f"Failed to generate plot of type '{plot_type}': {e}")
            raise
