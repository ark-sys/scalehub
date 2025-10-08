from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class WhiskerPlotStrategy(PlotStrategy):
    """Strategy for generating whisker/box plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (12, 10))
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a whisker/box plot."""
        boxplot_data = data.get("boxplot_data", [])
        # labels = data.get("labels", [])

        ylim_val = kwargs.get("ylim_val", None)
        comment = kwargs.get("comment", "")
        workload_objective = kwargs.get("workload_objective", -1)
        xlabel = kwargs.get("xlabel", "Number of Slots")
        ylabel = kwargs.get("ylabel", "Throughput (records/s)")
        filename = kwargs.get("filename", "whisker_plot.png")

        plt.rcParams["font.family"] = "serif"
        fig, ax = plt.subplots(figsize=self.figsize)

        # Create boxplot
        ax.boxplot(
            boxplot_data,
            # labels=labels,
            showfliers=False,
            meanline=True,
            whis=0,
            patch_artist=True,
            medianprops={"color": "black", "linewidth": 2.5},
            boxprops={"color": "black", "linewidth": 2.0, "facecolor": "white"},
            whiskerprops={"color": "black", "linewidth": 2.0},
            capprops={"color": "black", "linewidth": 2.0},
        )

        # Add grid
        ax.grid(axis="y", linestyle="--", alpha=0.3, color="gray")

        # Format y-axis
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.0f}"))

        # Labels
        ax.set_xlabel(xlabel, fontsize=self.fontsize + 15, labelpad=10)
        ax.set_ylabel(ylabel, fontsize=self.fontsize + 15, labelpad=10)

        # Tick styling
        plt.xticks(fontsize=self.tick_size + 14, alpha=0.8, rotation=45, ha="right")
        plt.yticks(fontsize=self.tick_size + 14, alpha=0.8)

        # Reference line
        if workload_objective != -1:
            ax.axhline(
                y=workload_objective,
                color="black",
                linestyle="--",
                linewidth=2.5,
                label="Workload objective",
            )
            ax.legend(
                loc="upper right",
                fontsize=self.fontsize,
                frameon=True,
                edgecolor="black",
            )

        # Y-axis limits
        if ylim_val:
            ax.set_ylim(0, ylim_val)

        # Title
        if comment:
            ax.set_title(f"{comment}", fontsize=self.fontsize)

        fig.tight_layout()

        save_path = self._plots_path / filename
        fig.savefig(save_path, dpi=600, bbox_inches="tight")
        plt.close(fig)

        return save_path
