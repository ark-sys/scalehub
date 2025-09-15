from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class BasicPlotStrategy(PlotStrategy):
    """Strategy for generating basic line plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (14, 10))
        self.linewidth = style_params.get("linewidth", 6)
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)
        self.legend_loc = style_params.get("legend_loc", "lower right")

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a basic line plot."""
        title = kwargs.get("title", "")
        xlabel = kwargs.get("xlabel", "")
        ylabel = kwargs.get("ylabel", "")
        ylim = kwargs.get("ylim", None)
        axhline = kwargs.get("axhline", None)
        filename = kwargs.get("filename", "basic_plot.png")

        plt.figure(figsize=self.figsize)
        plt.plot(data, linewidth=self.linewidth)
        plt.title(title, fontsize=self.fontsize)
        plt.xlabel(xlabel, fontsize=self.fontsize)
        plt.ylabel(ylabel, fontsize=self.fontsize)

        if ylim:
            plt.ylim(ylim)
        if axhline:
            plt.axhline(axhline)

        plt.tick_params(axis="both", labelsize=self.tick_size)
        plt.legend(loc=self.legend_loc)

        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()

        return save_path
