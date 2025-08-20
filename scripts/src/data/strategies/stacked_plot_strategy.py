from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MaxNLocator

from scripts.src.data.strategies.plot_strategy import PlotStrategy


class StackedPlotStrategy(PlotStrategy):
    """Strategy for generating stacked subplot plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (14, 10))
        self.linewidth = style_params.get("linewidth", 6)
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a stacked plot with multiple subplots."""
        title = kwargs.get("title", "")
        xlabel = kwargs.get("xlabel", "")
        ylabels_dict = kwargs.get("ylabels_dict", {})
        ylim_dict = kwargs.get("ylim_dict", {})
        axhline = kwargs.get("axhline", None)
        filename = kwargs.get("filename", "stacked_plot.png")

        fig, axs = plt.subplots(len(data), 1, figsize=self.figsize, sharex="all")

        for i, (label, series) in enumerate(data.items()):
            if isinstance(series, pd.DataFrame):
                for col in series.columns:
                    axs[i].plot(series[col], label=col, linewidth=self.linewidth)
            else:
                axs[i].plot(series, label=label, linewidth=self.linewidth)

            axs[i].set_title(f"{label}", fontsize=self.fontsize)

            if ylabels_dict and label in ylabels_dict:
                axs[i].set_ylabel(ylabels_dict[label], fontsize=self.fontsize)
            if ylim_dict and label in ylim_dict:
                axs[i].set_ylim(ylim_dict[label])
            if axhline:
                axs[i].axhline(axhline)

            axs[i].tick_params(axis="both", labelsize=self.tick_size)
            axs[i].yaxis.set_major_locator(MaxNLocator(4))
            axs[i].grid(axis="y", linestyle="--")

        axs[-1].set_xlabel(xlabel, fontsize=self.fontsize)

        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()

        return save_path
