from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm
from matplotlib.ticker import FuncFormatter

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class ThreeDPlotStrategy(PlotStrategy):
    """Strategy for generating 3D surface plots."""

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        self.figsize = style_params.get("figsize", (14, 10))
        self.fontsize = style_params.get("fontsize", 24)
        self.tick_size = style_params.get("tick_size", 22)

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a 3D surface plot."""
        x_p = data.get("x_data", [])
        y_p = data.get("y_data", [])
        z_p = data.get("z_data", [])

        title = kwargs.get("title", "")
        xlabel = kwargs.get("xlabel", "X")
        ylabel = kwargs.get("ylabel", "Y")
        zlabel = kwargs.get("zlabel", "Z")
        filename = kwargs.get("filename", "3d_plot.png")

        fig = plt.figure(figsize=self.figsize)
        ax = fig.add_subplot(projection="3d")

        # Create grid for surface plot
        x, y = np.meshgrid(np.unique(x_p), np.unique(y_p))
        z = np.zeros_like(x, dtype=float)

        # Fill Z with corresponding values
        for i in range(len(x_p)):
            xi = np.where(np.unique(x_p) == x_p[i])[0][0]
            yi = np.where(np.unique(y_p) == y_p[i])[0][0]
            z[yi, xi] = z_p[i]

        # Plot surface
        surf = ax.plot_surface(x, y, z, cmap=cm.viridis, edgecolor="none", alpha=0.5)

        # Add colorbar
        cbar = fig.colorbar(surf, ax=ax, shrink=0.5, aspect=15, pad=0.05)
        cbar.ax.tick_params(labelsize=self.fontsize)
        surf.set_clim(6000, 45000)
        cbar.ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.0f}"))

        # Plot wireframe
        ax.plot_wireframe(x, y, z, color="k", linewidth=0.5)

        # Mark highest and lowest points
        max_idx = np.unravel_index(np.argmax(z, axis=None), z.shape)
        min_idx = np.unravel_index(np.argmin(z, axis=None), z.shape)
        max_point = (x[max_idx], y[max_idx], z[max_idx])
        min_point = (x[min_idx], y[min_idx], z[min_idx])

        ax.scatter(*max_point, color="black", s=100, label="Max MST", marker="^")
        ax.scatter(*min_point, color="grey", s=100, label="Min MST", marker="v")

        # Add text annotations
        ax.text(
            *max_point,
            f"({int(max_point[0])}, {int(max_point[1])}) -> {int(max_point[2])}",
            fontsize=self.fontsize - 2,
            color="black",
            ha="center",
        )
        ax.text(
            *min_point,
            f"({int(min_point[0])}, {int(min_point[1])}) -> {int(min_point[2])}",
            fontsize=self.fontsize - 2,
            color="grey",
            ha="center",
        )

        # Styling
        ax.legend(fontsize=self.fontsize)
        ax.set_title(title)
        ax.set_xlabel(xlabel, fontsize=self.fontsize, labelpad=12)
        ax.set_ylabel(ylabel, fontsize=self.fontsize, labelpad=12)
        ax.set_zlabel(zlabel, fontsize=self.fontsize, labelpad=16)
        ax.set_zlim(0, z.max())

        # Set ticks
        ax.set_xticks(np.unique(x_p))
        ax.set_yticks(np.unique(y_p))
        ax.tick_params(axis="x", labelsize=self.tick_size - 2)
        ax.tick_params(axis="y", labelsize=self.tick_size - 2)
        ax.tick_params(axis="z", labelsize=self.tick_size)

        # Format z-axis
        ax.zaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.0f}"))
        ax.set_xlim(ax.get_xlim()[::-1])

        fig.tight_layout()

        save_path = self._plots_path / filename
        fig.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close(fig)

        self._logger.info(f"3D plot saved to {save_path}")
        return save_path
