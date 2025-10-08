import itertools
from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter, MaxNLocator

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class StackedFramesPlotStrategy(PlotStrategy):
    """Strategy for generating stacked frames with multiple series plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (9, 7))
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)
        self.linewidth = style_params.get("linewidth", 6)
        self.capsize = style_params.get("capsize", 10)
        self.legend_loc = style_params.get("legend_loc", "lower right")

        self.default_symbols = ["o", "v", "^", "<", ">", "s", "p", "*", "h", "H"]
        self.default_colors = ["tab:blue", "tab:orange", "g", "r", "c", "m", "y", "w"]

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate stacked frames plot with multiple series."""
        data_dict = data.get("data_dict", {})
        error_data_dict = data.get("error_data_dict", {})
        attributes_dict = kwargs.get("attributes_dict", {})

        title = kwargs.get("title", "")
        xlabel = kwargs.get("xlabel", "")
        filename = kwargs.get("filename", "stacked_frames_plot.png")

        num_subplots = len(data_dict)
        fig, axs = plt.subplots(num_subplots, 1, figsize=self.figsize, sharex="all")

        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        for i, (subplot_label, subplot_data) in enumerate(data_dict.items()):
            error_data = error_data_dict.get(subplot_label, None)

            for label, series in subplot_data.items():
                symbol = next(symbols)
                color = next(colors)

                if error_data and label in error_data:
                    axs[i].errorbar(
                        series.index,
                        series,
                        yerr=error_data[label],
                        fmt=symbol,
                        color=color,
                        linestyle="-",
                        capsize=self.capsize,
                        linewidth=self.linewidth,
                    )
                else:
                    axs[i].plot(
                        series.index,
                        series,
                        label=label,
                        marker=symbol,
                        color=color,
                        capsize=self.capsize,
                        linewidth=self.linewidth,
                    )

            # Apply subplot-specific attributes
            if attributes_dict and subplot_label in attributes_dict:
                subplot_attributes = attributes_dict[subplot_label]

                if "ylabel" in subplot_attributes:
                    axs[i].set_ylabel(
                        subplot_attributes["ylabel"], fontsize=self.fontsize + 2
                    )
                if "ylim" in subplot_attributes:
                    axs[i].set_ylim(subplot_attributes["ylim"])
                if "axhline" in subplot_attributes:
                    axs[i].axhline(
                        subplot_attributes["axhline"], color="r", linestyle="--"
                    )

            axs[i].legend(loc=self.legend_loc)
            axs[i].yaxis.set_major_locator(MaxNLocator(4))
            axs[i].tick_params(axis="both", labelsize=self.tick_size + 6)

            # Apply thousands formatter for large values
            max_value = max(
                series.max()
                for series in data_dict[subplot_label].values()
                if isinstance(series, pd.Series)
            )
            if max_value > 10000:
                axs[i].yaxis.set_major_formatter(
                    FuncFormatter(lambda x, p: f"{x:,.0f}")
                )

        axs[-1].set_xlabel(xlabel, fontsize=self.fontsize)
        axs[-1].xaxis.set_major_locator(MaxNLocator(integer=True))

        fig.tight_layout()

        save_path = self._plots_path / filename
        fig.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close(fig)

        return save_path
