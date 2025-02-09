import itertools
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt, cm
from matplotlib.ticker import FuncFormatter, MaxNLocator
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


class Plotter:
    default_symbols = ["o", "v", "^", "<", ">", "s", "p", "*", "h", "H"]
    default_colors = ["tab:blue", "tab:orange", "g", "r", "c", "m", "y", "w"]

    def __init__(
        self,
        log,
        fontsize=24,
        figsize=(12, 10),
        linewidth=6,
        capsize=10,
        markersize=15,
        tick_size=22,
        legend_loc="lower right",
        plots_path="plots",
    ):
        self.__log = log
        self.fontsize = fontsize
        self.figsize = figsize
        self.linewidth = linewidth
        self.capsize = capsize
        self.markersize = markersize
        self.tick_size = tick_size
        self.legend_loc = legend_loc
        self.plots_path = plots_path

    def generate_plot(
        self,
        data,
        title="",
        xlabel="",
        ylabel="",
        ylim=None,
        axhline=None,
        filename=None,
    ):
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
        if not filename:
            filename = f"{title}.png"
        plt.savefig(os.path.join(self.plots_path, filename))

    def generate_stacked_plot(
        self,
        data,
        title="",
        xlabel="",
        ylabels_dict=None,
        ylim_dict=None,
        axhline=None,
        filename=None,
    ):
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
        if not filename:
            filename = f"{title}.png"
        plt.savefig(os.path.join(self.plots_path, filename))

    def generate_single_frame_multiple_series_plot(
        self,
        ax1_data,
        ax1_error_data=None,
        ax2_data=None,
        ax2_error_data=None,
        title="",
        xlabel="",
        ylabels_dict=None,
        ylim=None,
        ylim2=None,
        axhline=None,
        filename=None,
        zoom_region=None,
    ):
        plt.figure(figsize=self.figsize)
        ax1 = plt.gca()
        ax2 = ax1.twinx() if ax2_data else None
        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        for label, series in ax1_data.items():
            symbol = next(symbols)
            color = next(colors)
            if label == "Predictions" and "Throughput" in ax1_data:
                percentage_error = (
                    (ax1_data["Predictions"] - ax1_data["Throughput"])
                    / ax1_data["Throughput"]
                ) * 100
                for x, y, error in zip(
                    series.index, ax1_data["Predictions"], percentage_error
                ):
                    ax1.annotate(
                        f"{error:.2f}%",
                        (x, y),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha="center",
                    )
            if ax1_error_data and label in ax1_error_data:
                ax1.errorbar(
                    series.index,
                    series,
                    yerr=ax1_error_data[label],
                    linewidth=self.linewidth,
                    linestyle="-",
                    label=label,
                    fmt=symbol,
                    color=color,
                    capsize=self.capsize,
                )
            else:
                ax1.plot(
                    series.index,
                    series,
                    linewidth=self.linewidth,
                    label=label,
                    marker=symbol,
                    color=color,
                    markersize=self.markersize,
                )

        if ax2_data:
            for label, series in ax2_data.items():
                symbol = next(symbols)
                color = next(colors)
                if ax2_error_data and label in ax2_error_data:
                    ax2.errorbar(
                        series.index,
                        series,
                        yerr=ax2_error_data[label],
                        linewidth=self.linewidth,
                        linestyle="--",
                        label=label,
                        fmt=symbol,
                        color=color,
                        capsize=self.capsize,
                    )
                else:
                    ax2.plot(
                        series.index,
                        series,
                        linewidth=self.linewidth,
                        label=label,
                        color=color,
                        marker=symbol,
                        markersize=self.markersize,
                    )

        if title:
            ax1.set_title(title, fontsize=self.fontsize)
        ax1.set_xlabel(xlabel, fontsize=self.fontsize)
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        # ax1.yaxis.grid(True)
        ax1.yaxis.set_major_formatter(FuncFormatter(self.__log.thousands_formatter))

        if ylabels_dict:
            ax1.set_ylabel(ylabels_dict["Throughput"], fontsize=self.fontsize)
            if ax2:
                ax2.set_ylabel(ylabels_dict["BusyTime"], fontsize=self.fontsize)
        if ylim:
            ax1.set_ylim(ylim)
        if axhline:
            ax1.axhline(axhline, color="r", linestyle="--")
        ax1.tick_params(axis="both", labelsize=self.tick_size + 6)
        if ylim2:
            ax2.set_ylim(ylim2)
        handles1, labels1 = ax1.get_legend_handles_labels()
        if ax2:
            ax2.tick_params(axis="y", labelsize=self.tick_size + 6)
            handles2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(
                handles1 + handles2,
                labels1 + labels2,
                fontsize=self.fontsize,
                loc=self.legend_loc,
            )
        else:
            # Upper right
            legend_loc = "upper right"
            ax1.legend(handles1, labels1, loc=legend_loc, fontsize=self.fontsize)
        ax1.set_xlim(left=1)
        plt.tight_layout()
        if zoom_region:
            x1, x2, y1, y2 = zoom_region
            axins = inset_axes(
                ax1,
                width="40%",
                height="40%",
                loc="lower left",
                bbox_to_anchor=(0.5, 0.1, 1, 1),
                bbox_transform=ax1.transAxes,
            )
            axins.set_xlim(x1, x2)
            axins.set_ylim(y1, y2)
            symbols = itertools.cycle(self.default_symbols)
            colors = itertools.cycle(self.default_colors)
            for label, series in ax1_data.items():
                symbol = next(symbols)
                color = next(colors)
                axins.plot(
                    series.index,
                    series,
                    linewidth=self.linewidth,
                    label=label,
                    marker=symbol,
                    color=color,
                    markersize=self.markersize,
                )
            axins.yaxis.set_major_formatter(
                FuncFormatter(self.__log.thousands_formatter)
            )
            axins.tick_params(axis="both", labelsize=self.tick_size)
            axins.grid(True)
            ax1.indicate_inset_zoom(axins)

        if not filename:
            filename = f"{title}.png"
        plt.savefig(os.path.join(self.plots_path, filename))
        plt.close()

    def generate_stacked_frames_multiple_series_plot(
        self,
        data_dict,
        error_data_dict,
        attributes_dict=None,
        title="",
        xlabel="",
        filename=None,
    ):
        num_subplots = len(data_dict)
        fig, axs = plt.subplots(num_subplots, 1, figsize=(9, 7), sharex="all")
        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        for i, (subplot_label, data) in enumerate(data_dict.items()):
            error_data = error_data_dict.get(subplot_label, None)
            for label, series in data.items():
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
            max_value = max(
                series.max()
                for series in data_dict[subplot_label].values()
                if isinstance(series, pd.Series)
            )
            if max_value > 10000:
                axs[i].yaxis.set_major_formatter(
                    FuncFormatter(self.__log.thousands_formatter)
                )

        axs[-1].set_xlabel(xlabel, fontsize=self.fontsize)
        axs[-1].xaxis.set_major_locator(MaxNLocator(integer=True))
        fig.tight_layout()
        if not filename:
            filename = f"{title}.png"
        plt.savefig(os.path.join(self.plots_path, filename))
        plt.close()

    def generate_3d_plot(self, x_p, y_p, z_p, title, xlabel, ylabel, zlabel, filename):
        fig = plt.figure(figsize=self.figsize)
        ax = fig.add_subplot(projection="3d")

        # Create a grid for the surface plot
        x, y = np.meshgrid(np.unique(x_p), np.unique(y_p))
        z = np.zeros_like(x, dtype=float)

        # Fill Z with the corresponding throughput values
        for i in range(len(x_p)):
            xi = np.where(np.unique(x_p) == x_p[i])[0][0]
            yi = np.where(np.unique(y_p) == y_p[i])[0][0]
            z[yi, xi] = z_p[i]

        # Plot the surface with a single color gradient
        surf = ax.plot_surface(x, y, z, cmap=cm.viridis, edgecolor="none", alpha=0.5)

        # Add color bar for the gradient
        cbar = fig.colorbar(surf, ax=ax, shrink=0.5, aspect=15, pad=0.05)

        cbar.ax.tick_params(labelsize=self.fontsize)

        surf.set_clim(6000, 45000)

        cbar.ax.yaxis.set_major_formatter(FuncFormatter(self.__log.thousands_formatter))

        # Plot the wireframe
        ax.plot_wireframe(x, y, z, color="k", linewidth=0.5)

        # Mark the highest and smallest points
        max_idx = np.unravel_index(np.argmax(z, axis=None), z.shape)
        min_idx = np.unravel_index(np.argmin(z, axis=None), z.shape)
        max_point = (x[max_idx], y[max_idx], z[max_idx])
        min_point = (x[min_idx], y[min_idx], z[min_idx])

        ax.scatter(*max_point, color="black", s=100, label="Max MST", marker="^")
        ax.scatter(*min_point, color="grey", s=100, label="Min MST", marker="v")

        # Add text to the max and min points with (cpu, mem)
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

        # Add legend
        ax.legend(fontsize=self.fontsize)

        ax.set_title(title)
        ax.set_xlabel(xlabel, fontsize=self.fontsize, labelpad=12)
        ax.set_ylabel(ylabel, fontsize=self.fontsize, labelpad=12)
        ax.set_zlabel(zlabel, fontsize=self.fontsize, labelpad=16)
        ax.set_zlim(0, z.max())

        # Fix scale of x-axis
        ax.set_xticks(np.unique(x_p))
        ax.set_yticks(np.unique(y_p))
        ax.tick_params(axis="x", labelsize=self.tick_size - 2)
        ax.tick_params(axis="y", labelsize=self.tick_size - 2)
        ax.tick_params(axis="z", labelsize=self.tick_size)

        # apply thousands formatter on z
        ax.zaxis.set_major_formatter(FuncFormatter(self.__log.thousands_formatter))

        ax.set_xlim(ax.get_xlim()[::-1])

        path_to_save = os.path.join(self.plots_path, filename)
        fig.tight_layout()
        plt.savefig(os.path.join(path_to_save))
        self.__log.info(f"3D plot saved to {path_to_save}")
        plt.close()

    def generate_whisker_plot(
        self,
        boxplot_data,
        labels,
        ylim_val,
        comment="",
        workload_objective=None,
        xlabel="Number of slots",
        ylabel="Throughput (records/s)",
    ):

        fig, ax = plt.subplots(figsize=self.figsize)
        ax.boxplot(
            boxplot_data,
            labels=labels,
            showfliers=False,
            meanline=True,
            whis=0,
        )

        ax.yaxis.set_major_formatter(FuncFormatter(self.__log.thousands_formatter))
        ax.set_xlabel(xlabel, fontsize=self.fontsize)
        ax.set_ylabel(ylabel, fontsize=self.fontsize)

        plt.xticks(fontsize=self.tick_size)
        ax.tick_params(axis="y", labelsize=self.tick_size)

        if workload_objective:
            ax.axhline(
                y=workload_objective,
                color="r",
                linestyle="--",
                label="Workload objective",
            )

        ax.set_ylim(0, ylim_val)

        # Add a legend
        # ax.legend(loc="upper right", fontsize=self.fontsize)

        # Get comment
        if comment:
            ax.set_title(f"{comment}", fontsize=self.fontsize)

        fig.tight_layout()
        output_path = os.path.join(
            self.plots_path, f"boxplot_{len(boxplot_data)}_runs_mean.png"
        )
        fig.savefig(output_path)
        plt.close(fig)
