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

import itertools
from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class SingleFramePlotStrategy(PlotStrategy):
    """Strategy for generating single frame multiple series plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (12, 10))
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)
        self.scientific_linewidth = style_params.get("scientific_linewidth", 4)
        self.scientific_markersize = style_params.get("scientific_markersize", 25)
        self.scientific_markeredgewidth = style_params.get("scientific_markeredgewidth", 2.0)
        self.capsize = style_params.get("capsize", 10)

        self.scientific_markers = ["o", "s", "^", "D", "v", "X", "P", "*", "H", "p"]
        self.scientific_colors = [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
            "#17becf",
        ]

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a single frame plot with multiple series."""
        ax1_data = data.get("ax1_data", {})
        ax1_error_data = data.get("ax1_error_data", {})
        ax2_data = data.get("ax2_data", {})
        ax2_error_data = data.get("ax2_error_data", {})

        title = kwargs.get("title", "")
        xlabel = kwargs.get("xlabel", "")
        ylabels_dict = kwargs.get("ylabels_dict", {})
        ylim = kwargs.get("ylim", None)
        ylim2 = kwargs.get("ylim2", None)
        axhline = kwargs.get("axhline", None)
        filename = kwargs.get("filename", "single_frame_plot.png")

        plt.rcParams["font.family"] = "serif"
        plt.figure(figsize=self.figsize)
        ax1 = plt.gca()
        ax2 = ax1.twinx() if ax2_data else None

        # Prepare markers and colors
        symbols = itertools.cycle(self.scientific_markers)
        colors = itertools.cycle(self.scientific_colors)

        color_dict = {label: next(colors) for label in ax1_data.keys()}
        marker_dict = {label: next(symbols) for label in ax1_data.keys()}

        # Plot primary axis data
        for label, series in ax1_data.items():
            color = color_dict[label]
            marker = marker_dict[label]
            error_data = ax1_error_data.get(label) if ax1_error_data else None

            if error_data is not None:
                ax1.errorbar(
                    series.index,
                    series,
                    yerr=error_data,
                    linewidth=self.scientific_linewidth,
                    linestyle="-",
                    label=label,
                    fmt=marker,
                    color=color,
                    capsize=self.capsize,
                    markersize=self.scientific_markersize,
                    markeredgecolor="black",
                    markeredgewidth=self.scientific_markeredgewidth,
                    ecolor=color,
                    zorder=3,
                )
            else:
                ax1.plot(
                    series.index,
                    series,
                    linewidth=self.scientific_linewidth,
                    label=label,
                    marker=marker,
                    color=color,
                    markersize=self.scientific_markersize,
                    markeredgecolor="black",
                    markeredgewidth=self.scientific_markeredgewidth,
                    zorder=2,
                )

        # Plot secondary axis data
        if ax2_data:
            for label, series in ax2_data.items():
                if label not in color_dict:
                    color_dict[label] = next(colors)
                    marker_dict[label] = next(symbols)

                color = color_dict[label]
                marker = marker_dict[label]
                error_data = ax2_error_data.get(label) if ax2_error_data else None

                if error_data is not None:
                    ax2.errorbar(
                        series.index,
                        series,
                        yerr=error_data,
                        linewidth=self.scientific_linewidth,
                        linestyle="--",
                        label=label,
                        fmt=marker,
                        color=color,
                        capsize=self.capsize,
                        markersize=self.scientific_markersize,
                        markeredgecolor="black",
                        markeredgewidth=self.scientific_markeredgewidth,
                        ecolor=color,
                        zorder=3,
                    )
                else:
                    ax2.plot(
                        series.index,
                        series,
                        linewidth=self.scientific_linewidth,
                        label=label,
                        color=color,
                        marker=marker,
                        markersize=self.scientific_markersize,
                        markeredgecolor="black",
                        markeredgewidth=self.scientific_markeredgewidth,
                        linestyle="--",
                        zorder=2,
                    )

        # Style the plot
        ax1.grid(True, linestyle="--", alpha=0.3, zorder=0)

        if title:
            ax1.set_title(title, fontsize=self.fontsize + 10)

        ax1.set_xlabel(xlabel, fontsize=self.fontsize + 10, labelpad=10)
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.0f}"))

        if ylabels_dict and "Throughput" in ylabels_dict:
            ax1.set_ylabel(ylabels_dict["Throughput"], fontsize=self.fontsize + 10, labelpad=10)
        if ax2 and ylabels_dict and "BusyTime" in ylabels_dict:
            ax2.set_ylabel(ylabels_dict["BusyTime"], fontsize=self.fontsize + 10, labelpad=10)

        if ylim:
            ax1.set_ylim(ylim)
        if ylim2 and ax2:
            ax2.set_ylim(ylim2)

        if axhline:
            ax1.axhline(
                axhline,
                color="black",
                linestyle="--",
                linewidth=2.5,
                label="Workload objective",
            )

        ax1.tick_params(axis="both", labelsize=self.tick_size + 12, pad=10)
        if ax2:
            ax2.tick_params(axis="y", labelsize=self.tick_size + 12)

        # Create legend
        handles1, labels1 = ax1.get_legend_handles_labels()
        if ax2:
            handles2, labels2 = ax2.get_legend_handles_labels()
            handles1.extend(handles2)
            labels1.extend(labels2)

        ax1.legend(
            handles1,
            labels1,
            fontsize=self.fontsize,
            loc="upper right",
            frameon=True,
            framealpha=0.9,
            edgecolor="black",
        )

        ax1.set_xlim(left=1)
        plt.tight_layout()

        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=600, bbox_inches="tight")
        plt.close()

        return save_path
