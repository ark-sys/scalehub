import itertools
import os
import re
from datetime import datetime

import networkx as nx
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator, FuncFormatter

from scripts.utils.Config import Config
from scripts.utils.Defaults import (
    DefaultKeys as Key,
    MAP_PIPELINE_DICT,
    JOIN_PIPELINE_DICT,
)
from scripts.utils.Logger import Logger


class Plotter:
    default_symbols = ["o", "v", "^", "<", ">", "s", "p", "*", "h", "H"]
    default_colors = ["tab:blue", "tab:orange", "g", "r", "c", "m", "y", "w"]

    def __init__(
        self,
        log,
        fontsize=22,
        figsize=(12, 10),
        linewidth=3,
        capsize=10,
        markersize=10,
        tick_size=20,
        legend_loc="lower right",
        plots_path="plots",
    ):
        """
        Initializes the Plotter class.

        Args:
            fontsize (int): Font size for plot titles and labels.
            figsize (tuple): Figure size for the plots.
            linewidth (int): Line width for the plots.
            capsize (int): Cap size for the error bars.
            markersize (int): Marker size for the scatter plots.
            tick_size (int): Size of the tick labels on both axes.
            legend_loc (str): Location of the legend.
        """
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
        """
        Generates a line plot.

        Args:
            data (list or array-like): Data to be plotted.
            title (str): Title for the plot.
            xlabel (str): Label for the x-axis.
            ylabel (str): Label for the y-axis.
            ylim (tuple): Tuple specifying the limits for the y-axis.
            axhline (float): Value at which to draw a horizontal line across the plot.
        """
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
        output_path = os.path.join(self.plots_path, filename)
        plt.savefig(output_path)

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
        """
        Generates a stacked plot with a shared x-axis.

        Args:
            data (dict): Dictionary where keys are labels for each subplot and values are the data series for each subplot.
            title (str): Title for the plot.
            xlabel (str): Label for the x-axis.
            ylabel (str): Label for the y-axis.
            ylim_dict (dict): Dictionary where keys are subplot labels and values are ylim tuples.
            axhline (float): Value at which to draw a horizontal line across the plot.
        """
        fig, axs = plt.subplots(len(data), 1, figsize=self.figsize, sharex="all")
        for i, (label, series) in enumerate(data.items()):
            # Check if we have multiple subtask index in the series
            if isinstance(series, pd.DataFrame):
                for col in series.columns:
                    axs[i].plot(series[col], label=col, linewidth=self.linewidth)
            else:
                # Otherwise, plot the series normally
                axs[i].plot(series, label=label, linewidth=self.linewidth)
            # axs[i].plot(series, linewidth=self.linewidth)
            axs[i].set_title(f"{label}", fontsize=self.fontsize)
            if ylabels_dict and label in ylabels_dict:
                axs[i].set_ylabel(ylabels_dict[label], fontsize=self.fontsize)
            if ylim_dict and label in ylim_dict:
                axs[i].set_ylim(ylim_dict[label])
            if axhline:
                axs[i].axhline(axhline)
            axs[i].tick_params(axis="both", labelsize=self.tick_size)
            axs[i].yaxis.set_major_locator(
                plt.MaxNLocator(4)
            )  # Set major ticks on y-axis
            axs[i].grid(axis="y", linestyle="--")  # Add grid lines at major ticks
        axs[-1].set_xlabel(xlabel, fontsize=self.fontsize)
        if not filename:
            filename = f"{title}.png"
        output_path = os.path.join(self.plots_path, filename)
        plt.savefig(output_path)

    def generate_single_plot_multiple_series(
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
    ):
        plt.figure(figsize=self.figsize)
        ax1 = plt.gca()
        ax2 = ax1.twinx() if ax2_data else None

        # Create an iterator from the list of symbols
        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        for label, series in ax1_data.items():
            # Get the next symbol from the iterator
            symbol = next(symbols)
            color = next(colors)
            # If the label is 'Predictions', calculate and plot the percentage error
            if label == "Predictions" and "Throughput" in ax1_data:
                # Calculate percentage error and add it to the plot
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
                )

        if ax2_data:
            for label, series in ax2_data.items():
                # Get the next symbol from the iterator
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
                    )

        ax1.set_title(title, fontsize=self.fontsize)
        ax1.set_xlabel(xlabel, fontsize=self.fontsize)
        # Set the x-axis to display all integers
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        # Use thousands formatter
        ax1.yaxis.set_major_formatter(FuncFormatter(self.__log.thousands_formatter))

        if ylabels_dict:
            ax1.set_ylabel(ylabels_dict["Throughput"], fontsize=self.fontsize)
            if ax2:
                ax2.set_ylabel(ylabels_dict["BusyTime"], fontsize=self.fontsize)
        if ylim:
            ax1.set_ylim(ylim)
        if axhline:
            ax1.axhline(axhline)
        ax1.tick_params(axis="both", labelsize=self.tick_size + 6)
        if ylim2:
            ax2.set_ylim(ylim2)
        # Add legend
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
            ax1.legend(handles1, labels1, loc=self.legend_loc)

        # Start x-axis from 1
        ax1.set_xlim(left=1)
        plt.tight_layout()
        if not filename:
            filename = f"{title}.png"
        output_path = os.path.join(self.plots_path, filename)
        plt.savefig(output_path)

    # def generate_stacked_plot_multiple_series(
    #     self,
    #     data_dict,
    #     error_data_dict=None,
    #     title="",
    #     xlabel="",
    #     ylabels_dict=None,
    #     ylim_dict=None,
    #     axhline=None,
    #     filename=None,
    # ):
    #     """
    #     Generates a stacked plot with multiple time series.
    #
    #     Args:
    #         data_dict (dict): Dictionary where keys are labels for each subplot and values are dictionaries mapping series labels to data series.
    #         error_data_dict (dict): Dictionary where keys are labels for each subplot and values are dictionaries mapping series labels to error data series.
    #         title (str): Title for the plot.
    #         xlabel (str): Label for the x-axis.
    #         ylabels_dict (dict): Dictionary where keys are labels for each subplot and values are labels for the y-axis.
    #         ylim_dict (dict): Dictionary where keys are labels for each subplot and values are tuples specifying the limits for the y-axis.
    #         axhline (float): Value at which to draw a horizontal line across the plot.
    #     """
    #     num_subplots = len(data_dict)
    #     fig, axs = plt.subplots(num_subplots, 1, figsize=self.figsize, sharex=True)
    #
    #     # Create an iterator from the list of symbols
    #     symbols = itertools.cycle(self.default_symbols)
    #
    #     for i, (subplot_label, data) in enumerate(data_dict.items()):
    #         error_data = error_data_dict.get(subplot_label, None)
    #         for label, series in data.items():
    #             # Get the next symbol from the iterator
    #             symbol = next(symbols)
    #
    #             # Check if series is a pandas Series or DataFrame
    #             if isinstance(series, (pd.Series, pd.DataFrame)):
    #                 print("Plotting series", label, "in subplot", subplot_label)
    #                 if error_data and label in error_data:
    #                     axs[i].errorbar(
    #                         series.index,
    #                         series,
    #                         yerr=error_data[label],
    #                         fmt=symbol,
    #                         capsize=self.capsize,
    #                     )
    #                 else:
    #                     axs[i].plot(
    #                         series.index,
    #                         series,
    #                         label=label,
    #                         marker=symbol,
    #                     )
    #             else:
    #                 print(
    #                     f"Warning: Expected series to be a 'pandas.Series' or 'pandas.DataFrame' but got '{type(series)}' instead."
    #                 )
    #
    #         # axs[i].set_title(subplot_label, fontsize=self.fontsize)
    #         axs[i].set_ylabel(
    #             ylabels_dict.get(subplot_label, ""), fontsize=self.fontsize
    #         )
    #         if ylim_dict and subplot_label in ylim_dict:
    #             axs[i].set_ylim(ylim_dict[subplot_label])
    #         if axhline:
    #             axs[i].axhline(axhline)
    #         axs[i].tick_params(axis="both", labelsize=self.tick_size)
    #         axs[i].legend(loc=self.legend_loc)
    #
    #     axs[-1].set_xlabel(xlabel, fontsize=self.fontsize)
    #     # fig.suptitle(title, fontsize=self.fontsize)
    #
    #     if not filename:
    #         filename = f"{title}.png"
    #     output_path = os.path.join(self.plots_path, filename)
    #     plt.savefig(output_path)

    def generate_stacked_plot_multiple_series(
        self,
        data_dict,
        error_data_dict,
        attributes_dict=None,
        title="",
        xlabel="",
        filename=None,
    ):
        num_subplots = len(data_dict)

        custom_figsize = (9, 7)
        fig, axs = plt.subplots(num_subplots, 1, figsize=custom_figsize, sharex=True)

        # Create an iterator from the list of symbols
        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        for i, (subplot_label, data) in enumerate(data_dict.items()):
            error_data = error_data_dict.get(subplot_label, None)
            for label, series in data.items():
                # Check if series is a pandas Series or DataFrame
                if isinstance(series, (pd.Series, pd.DataFrame)):
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
                else:
                    print(
                        f"Warning: Expected series to be a 'pandas.Series' or 'pandas.DataFrame' but got '{type(series)}' instead."
                    )

            # Apply attributes from attributes_dict
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
            axs[i].yaxis.set_major_locator(
                plt.MaxNLocator(4)
            )  # Set major ticks on y-axis
            # axs[i].grid(axis="y", linestyle="--")
            axs[i].tick_params(axis="both", labelsize=self.tick_size + 6)
            # If the maximum value across all series is greater than 10000, use thousands formatter
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
        output_path = os.path.join(self.plots_path, filename)
        plt.savefig(output_path)

    def generate_stacked_plot_multiple_series_lazyass(
        self,
        data_dict,
        error_data_dict,
        attributes_dict=None,
        title="",
        xlabel="",
        filename=None,
    ):
        num_subplots = len(data_dict)

        custom_figsize = (9, 7)
        fig, axs = plt.subplots(num_subplots, 1, figsize=custom_figsize, sharex=True)

        # Create an iterator from the list of symbols
        symbols = itertools.cycle(self.default_symbols)
        colors = itertools.cycle(self.default_colors)

        # Create a dictionary to map latency values to colors and symbols
        latency_to_color_and_symbol = {}

        for i, (subplot_label, data) in enumerate(data_dict.items()):
            error_data = error_data_dict.get(subplot_label, None)
            for label, series in data.items():
                # Extract the latency value from the label
                latency = label.split("_")[-1].split("ms")[0]

                # If this latency value hasn't been seen before, assign it a color and symbol
                if latency not in latency_to_color_and_symbol:
                    latency_to_color_and_symbol[latency] = (next(colors), next(symbols))

                color, symbol = latency_to_color_and_symbol[latency]

                # Plot the series with the assigned color and symbol
                axs[i].plot(
                    series.index,
                    series,
                    label=latency,
                    color=color,
                    marker=symbol,
                    markersize=self.capsize,
                    linewidth=self.linewidth,
                )

                # # If there is error data for this series, plot it as well
                # if error_data and label in error_data:
                #     axs[i].errorbar(
                #         series.index,
                #         series,
                #         yerr=error_data[label],
                #         fmt=symbol,
                #         capsize=self.capsize + 6,
                #         color=color,
                #         elinewidth=0.2,
                #     )

            # Set subplot attributes
            if attributes_dict and subplot_label in attributes_dict:
                axs[i].set_ylabel(
                    attributes_dict[subplot_label].get("ylabel", ""),
                    fontsize=self.fontsize,
                )
                axs[i].set_ylim(attributes_dict[subplot_label].get("ylim", (0, 1)))
                axs[i].axhline(
                    attributes_dict[subplot_label].get("axhline", 0),
                    color="r",
                    linestyle="--",
                )
                axs[i].tick_params(axis="both", labelsize=self.tick_size)
                axs[i].legend(loc="upper right", fontsize=16, title="Latency (ms)")
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
        output_path = os.path.join(self.plots_path, filename)
        plt.savefig(output_path)


class DataEval:
    def __init__(self, log: Logger, exp_path: str):
        self.__log = log
        # Path to experiment folder
        self.exp_path = exp_path
        # Path to log file
        self.log_file = os.path.join(self.exp_path, "exp_log.txt")
        # Path to transscale log file
        self.transscale_log = os.path.join(self.exp_path, "transscale_log.txt")

        # Create an export folder if it doesn't exist
        self.export_path = os.path.join(self.exp_path, "export")
        if not os.path.exists(self.export_path):
            os.makedirs(self.export_path)

        # Create a folder for plots if it doesn't exist
        self.plots_path = os.path.join(self.exp_path, "plots")
        if not os.path.exists(self.plots_path):
            os.makedirs(self.plots_path)

        # Create a Plotter object
        self.plotter = Plotter(self.__log, plots_path=self.plots_path)

        # Parse configuration file for experiment
        self.conf: Config = Config(log, self.log_file)

        # Time to skip in seconds at the beginning and the end of a parallelism region
        self.start_skip = 30
        self.end_skip = 30

        if not hasattr(self, "final_df"):
            self.final_df = pd.read_csv(
                os.path.join(self.exp_path, "final_df.csv"),
                index_col=0,
                header=[0, 1, 2],
            )

    def __export_predictions(self) -> list[tuple[int, int, int, float]]:
        try:
            with open(self.transscale_log, "r") as file:
                log_content = file.read()

            # Define the regular expression pattern
            pattern = r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\] \[COMBO_CTRL\] Reconf: Scale (Up|Down) (.*) from par (\d+)([\s\S]*?)\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\] \[RES_MNGR\] Re-configuring PARALLELISM[\s\S].*\n.*Target Parallelism: (\d+)"
            # Extract the matches
            predictions = []
            for match in re.finditer(pattern, log_content):
                # Extract the match
                current_parallelism = int(match.group(4))
                target_parallelism = int(match.group(6))
                prediction_block = match.group(5)
                operator = match.group(3)
                time = match.group(1)

                operator_name = self.conf.get(Key.Experiment.task_name)

                if operator_name in operator:
                    throughput_pattern = rf".*par, transp: {target_parallelism}, 1 target tput: (\d+) new_tput %: (\d+\.\d+)"
                    # Extract the throughput from prediction block
                    throughput_match = re.search(throughput_pattern, prediction_block)
                    if throughput_match:
                        throughput = float(throughput_match.group(2))
                        # time is in format "2024-02-04T12:00:00", transform it to milliseconds
                        time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S").timestamp()
                        predictions.append(
                            (
                                int(time + 3600),
                                current_parallelism,
                                target_parallelism,
                                throughput,
                            )
                        )
        except:
            self.__log.error("Failed to extract predictions from transscale log.")
            predictions = []
        return predictions

    def eval_mean_stderr(self):
        # Load the DataFrame
        df = self.final_df

        # Reduce headers from [0,1,2] to 0
        if df.columns.nlevels > 2:
            df.columns = df.columns.droplevel([1, 2])

        # Identify the columns related to 'numRecordsInPerSecond'
        numRecordsInPerSecond_cols = [
            col
            for col in df.columns
            if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
        ]

        busyTimePerSecond_cols = [
            col
            for col in df.columns
            if "flink_taskmanager_job_task_busyTimeMsPerSecond" in str(col)
        ]

        backpressureTimePerSecond_cols = [
            col
            for col in df.columns
            if "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond" in str(col)
        ]

        # Add a new column 'BackpressureTime' to the DataFrame which is the mean of 'hardBackPressuredTimeMsPerSecond' across all subtasks
        df["BackpressureTime"] = df[backpressureTimePerSecond_cols].mean(axis=1)

        # Add a new column 'BusyTime' to the DataFrame which is the mean of 'busyTimePerSecond' across all subtasks
        df["BusyTime"] = df[busyTimePerSecond_cols].mean(axis=1)

        # Add a new column 'Throughput' to the DataFrame which is the sum of 'numRecordsInPerSecond' across all subtasks
        df["Throughput"] = df[numRecordsInPerSecond_cols].sum(axis=1)

        # Convert the index to a DatetimeIndex
        df.index = pd.to_datetime(arg=df.index, unit="s")

        # For each group, remove the first self.start_skip seconds and the last self.end_skip seconds
        df_filtered = df.groupby(["Parallelism"]).apply(
            lambda group: group[
                (
                    group.index
                    >= group.index.min() + pd.Timedelta(seconds=self.start_skip)
                )
                & (
                    group.index
                    <= group.index.max() - pd.Timedelta(seconds=self.end_skip)
                )
            ]
        )

        df_filtered = df_filtered.drop(columns=["Parallelism"])
        df_filtered.reset_index(inplace=True)

        # Calculate mean and standard error
        df_final = df_filtered.groupby("Parallelism")[
            ["Throughput", "BusyTime", "BackpressureTime"]
        ].agg(["mean", lambda x: np.std(x) / np.sqrt(x.count())])

        # Rename the columns
        df_final.columns = [
            "Throughput",
            "ThroughputStdErr",
            "BusyTime",
            "BusyTimeStdErr",
            "BackpressureTime",
            "BackpressureTimeStdErr",
        ]

        # Extract predictions from transscale log
        predictions = self.__export_predictions()

        if len(predictions) == 0:
            self.__log.warning("No predictions found in transscale log.")
        else:
            # Add the predictions to the DataFrame
            df_final["Predictions"] = np.nan
            for prediction in predictions:
                time, current_parallelism, target_parallelism, throughput = prediction
                df_final.loc[target_parallelism, "Predictions"] = throughput

        # Save the DataFrame to a CSV file
        df_final.to_csv(os.path.join(self.exp_path, "mean_stderr.csv"))

        return df_final

    def eval_experiment_plot(self):

        # Create vertically stacked plots sharing the x-axis (Timestamp)
        # On the first plot, plot the throughput in
        # On the second plot, plot the busytime per second
        # On the third plot, plot the backpressure time per second

        # Create dataframe from final_df containing only numRecordsInPerSecond, busyTimeMsPerSecond and hardBackPressuredTimeMsPerSecond columns
        numRecordsInPerSecond_df = self.final_df.filter(
            regex="flink_taskmanager_job_task_numRecordsInPerSecond"
        )
        busyTimePerSecond_df = self.final_df.filter(
            regex="flink_taskmanager_job_task_busyTimeMsPerSecond"
        )
        hardBackPressuredTimeMsPerSecond_df = self.final_df.filter(
            regex="flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"
        )

        data = {
            "Throughput": numRecordsInPerSecond_df,
            "BusyTime": busyTimePerSecond_df,
            "BackpressureTime": hardBackPressuredTimeMsPerSecond_df,
        }

        # Prepare ylim dictionary
        ylim_dict = {
            "Throughput": (0, 30000),
            "BusyTime": (0, 1200),
            "BackpressureTime": (0, 1200),
        }

        ylabels_dict = {
            "Throughput": "Records/s",
            "BusyTime": "ms/s",
            "BackpressureTime": "ms/s",
        }

        # Generate stacked plot*
        self.plotter.generate_stacked_plot(
            data,
            title="Experiment Plot",
            xlabel="Time (s)",
            ylabels_dict=ylabels_dict,
            ylim_dict=ylim_dict,
            filename="experiment_plot.png",
        )

    # Eval Summary Plot
    def eval_summary_plot(self):
        # Prepare the data
        dataset = self.eval_mean_stderr()

        # Prepare data for multiple series plot
        ax1_data = {
            "Throughput": dataset["Throughput"],
        }

        ax1_error_data = {
            "Throughput": dataset["ThroughputStdErr"],
        }

        ax2_data = {
            "BusyTime": dataset["BusyTime"],
            "BackpressureTime": dataset["BackpressureTime"],
        }

        ax2_error_data = {
            "BusyTime": dataset["BusyTimeStdErr"],
            "BackpressureTime": dataset["BackpressureTimeStdErr"],
        }

        # Check if predictions are available
        if "Predictions" in dataset.columns:
            ax1_data["Predictions"] = dataset["Predictions"]

        # Get operator name from config file
        jobname = self.conf.get(Key.Experiment.job_file)
        # Extract the operator name from the job name
        if "join" in jobname:
            if "kk" in jobname:
                operator_name = "Join (kk)"
            elif "kv" in jobname:
                operator_name = "Join (kv)"
            else:
                operator_name = "Join"
        elif "map" in jobname:
            operator_name = "Map"
        else:
            operator_name = "Unknown"

        # Check if latency is enabled, and if so, check if jitter is enabled
        if self.conf.get(Key.Experiment.Chaos.enable) == "true":
            jitter = self.conf.get(Key.Experiment.Chaos.delay_jitter_ms)
            if int(jitter) > 0:
                title = f"{operator_name} - Latency and Jitter"
            else:
                title = f"{operator_name} - Latency"
        else:
            title = f"{operator_name} - No Latency"

        # Call generate_single_plot_multiple_series() to plot the data
        self.plotter.generate_single_plot_multiple_series(
            ax1_data,
            ax1_error_data,
            ax2_data,
            ax2_error_data,
            title=title,
            xlabel="Parallelism Level",
            ylabels_dict={
                "Throughput": "Records/s",
                "BusyTime": "ms/s",
                "BackpressureTime": "ms/s",
            },
            ylim=(0, 120000),
            ylim2=(0, 1200),
            filename="summary_plot.png",
        )

    # TODO - Adapt this method with the new way of plotting
    def eval_plot_with_checkpoints(self):
        # dataset = self.eval_mean_with_state_size()
        dataset = self.final_df

        # Convert lastCheckpointSize to MB
        dataset["flink_jobmanager_job_lastCheckpointSize"] = (
            dataset["flink_jobmanager_job_lastCheckpointSize"] / 1024 / 1024
        )

        # Stacked plot with numRecordsInPerSecond, lastCheckpointSize and busyTimePerSecond
        fig, axs = plt.subplots(3, 1, figsize=(10, 10), sharex="all")
        numRecordsInPerSecond_cols = [
            col
            for col in dataset.columns
            if any("numRecordsInPerSecond" in str(item) for item in col)
        ]
        busyTimePerSecond_cols = [
            col
            for col in dataset.columns
            if any("busyTimeMsPerSecond" in str(item) for item in col)
        ]
        # Plot numRecordsInPerSecond for each subtask
        for col in numRecordsInPerSecond_cols:
            subtask_index = col[-1]
            axs[0].plot(dataset.index, dataset[col], label=subtask_index)

        axs[0].set_title("numRecordsInPerSecond per subtask")
        axs[0].set_ylabel("Records/s")
        axs[0].set_xlabel("Parallelism")

        # Plot busyTimePerSecond for each subtask
        for col in busyTimePerSecond_cols:
            subtask_index = col[-1]
            axs[1].plot(dataset.index, dataset[col], label=subtask_index)
        axs[1].set_title("busyTimePerSecond per subtask")
        axs[1].set_ylabel("ms/s")
        axs[1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # Plot lastCheckpointSize
        axs[2].plot(dataset.index, dataset["flink_jobmanager_job_lastCheckpointSize"])
        axs[2].set_title("lastCheckpointSize")
        axs[2].set_ylabel("MB")
        plt.show()

        # Save the plot to a file
        plot_file = f"{self.plots_path}/experiment_plot_with_checkpoints.png"
        fig.savefig(plot_file)

    def eval_mean_with_backpressure(self):
        dataset = self.eval_mean_stderr()
        dataset = dataset.drop(0)

        # Divide values of BackpressureTime and BackpressureTimeStdErr by 10 to get percentage
        dataset["BackpressureTime"] = dataset["BackpressureTime"] / 10
        dataset["BackpressureTimeStdErr"] = dataset["BackpressureTimeStdErr"] / 10

        # Prepare data for stacked plot
        data = {
            "subplot1": {
                "Throughput": dataset["Throughput"],
            },
            "subplot2": {
                "BackpressureTime": dataset["BackpressureTime"],
            },
        }
        # Prepare errordata for stacked plot
        error_data = {
            "subplot1": {
                "Throughput": dataset["ThroughputStdErr"],
            },
            "subplot2": {
                "BackpressureTime": dataset["BackpressureTimeStdErr"],
            },
        }

        # Prepare attributes for stacked plot
        attributes_dict = {
            "subplot1": {
                "ylabel": "Throughput\n(Records/s)",
                "ylim": (0, 120000),
                "axhline": 100000,
            },
            "subplot2": {
                "ylabel": "Backpressure\n(%)",
                "ylim": (0, 100),
            },
        }

        # Generate stacked plot
        self.plotter.generate_stacked_plot_multiple_series(
            data,
            error_data,
            attributes_dict=attributes_dict,
            xlabel="Operator Parallelism",
            filename="stacked_mean_with_backpressure.png",
        )

        return data, error_data

    def plot_dag(self):
        # Create a directed graph
        G = nx.DiGraph()

        # Get the name of the experiment
        experiment_name = self.conf.get(Key.Experiment.name)

        # Choose the appropriate pipeline dictionary based on the experiment name
        if experiment_name == "Map":
            pipeline_dict = MAP_PIPELINE_DICT
            pos = {
                "Source:_Source": (0, 0),
                "Map": (1, 0),
                "Sink:_Sink": (2, 0),
            }
        elif experiment_name == "Join":
            pipeline_dict = JOIN_PIPELINE_DICT
            pos = {
                "Source:_Source1": (0, 0),
                "Source:_Source2": (0, 1),
                "Timestamps_Watermarks____Map": (1, 0.5),
                "TumblingEventTimeWindows____Timestamps_Watermarks": (2, 0.5),
                "Sink:_Sink": (3, 0.5),
            }
        else:
            raise ValueError(f"Unknown experiment name: {experiment_name}")
            # First, add all nodes to the graph
        for value in pipeline_dict.values():
            if isinstance(value, tuple):
                for v in value:
                    if not G.has_node(v):
                        G.add_node(v, label=v)
            else:
                if not G.has_node(value):
                    G.add_node(value, label=value)

        # Then, add all edges to the graph
        for key, value in pipeline_dict.items():
            if key + 1 in pipeline_dict:
                next_value = pipeline_dict[key + 1]
                if isinstance(value, tuple):
                    for v in value:
                        G.add_edge(v, next_value)
                else:
                    G.add_edge(value, next_value)

        # Draw the graph
        labels = nx.get_node_attributes(G, "label")
        nx.draw(G, pos, with_labels=False, labels=labels, arrows=True)
        # Plot node labels under the nodes
        labels_pos = {
            k: (v[0], v[1] - 0.05 - (0.025 * (len(k) // 10))) for k, v in pos.items()
        }

        nx.draw_networkx_labels(G, labels_pos, labels, font_size=10, font_color="black")

        # Save the plot to a file
        plt.savefig(f"{self.plots_path}/dag.png")
        # Return the graph with formatted labels
        return G

    def eval_buffers_plot(self):
        # Load the DataFrame and set metric (header 0) and task (header 1) names as headers
        job_metrics_df = pd.read_csv(
            os.path.join(self.exp_path, "job_metrics_df.csv"),
            index_col=0,
            header=[0, 1, 2],
        )

        if self.conf.get(Key.Experiment.name) == "Join":
            # Build df for metric flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength and task Timestamps_Watermarks____Map
            outpre_df = job_metrics_df[
                "flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength"
            ]["Timestamps_Watermarks____Map"]

            inop_df = job_metrics_df[
                "flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength"
            ]["TumblingEventTimeWindows____Timestamps_Watermarks"]

            # Build df of metric flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs for tasks Timestamps_Watermarks____Map and TumblingEventTimeWindows____Timestamps_Watermarks
            etcb_timestamps_df = job_metrics_df[
                "flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs"
            ]["Timestamps_Watermarks____Map"]

            etcb_tumbling_df = job_metrics_df[
                "flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs"
            ]["TumblingEventTimeWindows____Timestamps_Watermarks"]
        elif self.conf.get(Key.Experiment.name) == "Map":
            # Build df for metric flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength and task Timestamps_Watermarks____Map
            outpre_df = job_metrics_df[
                "flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength"
            ]["Source:_Source"]

            inop_df = job_metrics_df[
                "flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength"
            ]["Map"]

            # Build df of metric flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs for tasks Timestamps_Watermarks____Map and TumblingEventTimeWindows____Timestamps_Watermarks
            etcb_timestamps_df = job_metrics_df[
                "flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs"
            ]["Source:_Source"]

            etcb_tumbling_df = job_metrics_df[
                "flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs"
            ]["Map"]
        else:
            self.__log.error(
                f"Unknown experiment name: {self.conf.get(Key.Experiment.name)}"
            )
            return

        # Sum dataframes
        etcb_timestamps_df = etcb_timestamps_df.sum(axis=1)
        etcb_tumbling_df = etcb_tumbling_df.sum(axis=1)

        # Concatenate dataframes
        estime_df = pd.concat([etcb_timestamps_df, etcb_tumbling_df], axis=1)

        tpt_df = (
            self.final_df.filter(
                regex="flink_taskmanager_job_task_numRecordsInPerSecond"
            )
            .sum(axis=1)
            .sort_index()
        )

        busyness_df = self.final_df.filter(
            regex="flink_taskmanager_job_task_busyTimeMsPerSecond"
        ).mean(axis=1)

        backpressure_df = self.final_df.filter(
            regex="flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"
        )

        inop_df = inop_df.sum(axis=1)

        # Evaluate derivative of the input queue length

        # Divide timestamp by 1000 to get seconds, and start from 0
        outpre_df.index = (outpre_df.index - outpre_df.index[0]) / 1000
        inop_df.index = (inop_df.index - inop_df.index[0]) / 1000
        estime_df.index = (estime_df.index - estime_df.index[0]) / 1000

        # Sort on timestamp
        outpre_df = outpre_df.sort_index()
        inop_df = inop_df.sort_index()
        estime_df = estime_df.sort_index()

        # Divide all values in estime_df by 1000000 to get seconds
        estime_df = estime_df / 1000000

        data = {
            "Throughput": tpt_df,
            "BusyTime": busyness_df,
            "BackpressureTime": backpressure_df,
            "Output Queue Length": outpre_df,
            "Input Queue Length": inop_df,
            "Estimated Time to Consume Buffers": estime_df,
        }

        # Prepare ylim dictionary
        ylim_dict = {
            "Throughput": (0, 120000),
            "BusyTime": (0, 1200),
            "BackpressureTime": (0, 1200),
            "Output Queue Length": (0, 40),
            "Input Queue Length": (0, 120),
            "Estimated Time to Consume Buffers": (0, 1200),
        }

        ylabels_dict = {
            "Throughput": "Records/s",
            "BusyTime": "ms/s",
            "BackpressureTime": "ms/s",
            "Output Queue Length": "Buffers",
            "Input Queue Length": "Buffers",
            "Estimated Time to Consume Buffers": "ms",
        }

        # Generate stacked plot
        self.plotter.generate_stacked_plot(
            data,
            title="Buffers Plot",
            xlabel="Time (s)",
            ylabels_dict=ylabels_dict,
            ylim_dict=ylim_dict,
            filename="buffers_plot.png",
        )

    def eval_mean_with_buffers(self):
        data = self.final_df
        job_metrics_df = pd.read_csv(
            os.path.join(self.exp_path, "job_metrics_df.csv"),
            index_col=0,
            header=[0, 1, 2],
        )
        job_metrics_df.sort_index(inplace=True)

        # Start index from 0
        job_metrics_df.index = job_metrics_df.index - job_metrics_df.index.min()

        # Concat dataframes
        df = data.join(job_metrics_df)

        # Get throughput columns
        df["Throughput"] = df.filter(
            regex="flink_taskmanager_job_task_numRecordsInPerSecond"
        ).sum(axis=1)

        # Get busyness columns
        df["Busyness"] = df.filter(
            regex="flink_taskmanager_job_task_busyTimeMsPerSecond"
        ).mean(axis=1)

        # Get backpressure columns
        df["Backpressure"] = df.filter(
            regex="flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"
        ).mean(axis=1)

        filtered_output_queue_df = df.filter(
            regex="flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength"
        )
        filtered_input_queue_df = df.filter(
            regex="flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength"
        )

        filtered_etcb_df = df.filter(
            regex="flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs"
        )

        # Get input and output queues columns
        if self.conf.get(Key.Experiment.name) == "Join":
            self.__log.info("Join experiment")

            timestamp_cols = [
                col
                for col in filtered_output_queue_df.columns
                if "Timestamps_Watermarks____Map" in col
            ]

            df["OutputQueuePredecessor"] = filtered_output_queue_df[timestamp_cols].sum(
                axis=1
            )

            tumbling_cols = [
                col
                for col in filtered_input_queue_df.columns
                if "TumblingEventTimeWindows____Timestamps_Watermarks" in col
            ]

            df["InputQueueOperator"] = filtered_input_queue_df[tumbling_cols].sum(
                axis=1
            )

            timestamp_etcb_cols = [
                col
                for col in filtered_etcb_df.columns
                if "Timestamps_Watermarks____Map" in col
            ]

            df["EstimatedTimeCBPre"] = filtered_etcb_df[timestamp_etcb_cols].sum(axis=1)

            tumbling_etcb_cols = [
                col
                for col in filtered_etcb_df.columns
                if "TumblingEventTimeWindows____Timestamps_Watermarks" in col
            ]

            df["EstimatedTimeCBOp"] = filtered_etcb_df[tumbling_etcb_cols].sum(axis=1)

        elif self.conf.get(Key.Experiment.name) == "Map":
            self.__log.info("Map experiment")

            source_cols = [
                col
                for col in filtered_output_queue_df.columns
                if "Source:_Source" in col
            ]

            df["OutputQueuePredecessor"] = filtered_output_queue_df[source_cols].sum(
                axis=1
            )

            map_cols = [col for col in filtered_input_queue_df.columns if "Map" in col]

            df["InputQueueOperator"] = filtered_input_queue_df[map_cols].sum(axis=1)

            source_etcb_cols = [
                col for col in filtered_etcb_df.columns if "Source:_Source" in col
            ]

            df["EstimatedTimeCBPre"] = filtered_etcb_df[source_etcb_cols].sum(axis=1)

            map_etcb_cols = [col for col in filtered_etcb_df.columns if "Map" in col]

            df["EstimatedTimeCBOp"] = filtered_etcb_df[map_etcb_cols].sum(axis=1)
        else:
            self.__log.error(
                f"Unknown experiment name: {self.conf.get(Key.Experiment.name)}"
            )
            return

        # Convert the index to a DatetimeIndex
        df.index = pd.to_datetime(arg=df.index, unit="s")

        grouped_df = df.groupby("Parallelism")
        # For each group, remove the first self.start_skip seconds and the last self.end_skip seconds
        df_filtered = grouped_df.apply(
            lambda group: group[
                (
                    group.index
                    >= group.index.min() + pd.Timedelta(seconds=self.start_skip)
                )
                & (
                    group.index
                    <= group.index.max() - pd.Timedelta(seconds=self.end_skip)
                )
            ]
        )

        df_filtered = df_filtered.drop(columns=["Parallelism"])
        df_filtered.reset_index(inplace=True)
        # Calculate mean and standard error
        df_final = df_filtered.groupby("Parallelism")[
            [
                "Throughput",
                "Busyness",
                "Backpressure",
                "OutputQueuePredecessor",
                "InputQueueOperator",
                "EstimatedTimeCBPre",
                "EstimatedTimeCBOp",
            ]
        ].agg(["mean", lambda x: np.std(x) / np.sqrt(x.count())])

        # Rename the columns
        df_final.columns = [
            "Throughput",
            "ThroughputStdErr",
            "Busyness",
            "BusynessStdErr",
            "Backpressure",
            "BackpressureStdErr",
            "OutputQueuePredecessor",
            "OutputQueuePredecessorStdErr",
            "InputQueueOperator",
            "InputQueueOperatorStdErr",
            "EstimatedTimeCBPre",
            "EstimatedTimeCBPreStdErr",
            "EstimatedTimeCBOp",
            "EstimatedTimeCBOpStdErr",
        ]

        print(df_final)


class GroupedDataEval:
    skip = 30
    experiments = ["Join-kk", "Join-kv", "Map"]
    types = ["no_lat", "latency", "latency_jitter"]
    high_latency = ["25ms", "50ms"]

    def __init__(self, log: Logger, base_path):
        self.base_path = base_path
        self.__log: Logger = log
        self.plotter = Plotter(self.__log, plots_path=self.base_path)

    def eval_mean_with_backpressure_multiple(self):
        # This dict will contain
        data_dict = {}
        data_error_dict = {}
        for latency in self.high_latency:
            full_path = os.path.join(self.base_path, latency)

            data_obj = DataEval(self.__log, full_path)

            data, error_data = data_obj.eval_mean_with_backpressure()

            data_dict[latency] = data
            data_error_dict[latency] = error_data

            # Prepare the final dataset and error dataset
        dataset = {"subplot1": {}, "subplot2": {}}
        dataset_error = {"subplot1": {}, "subplot2": {}}

        # Iterate over the data_dict and data_error_dict
        for latency, data in data_dict.items():
            for subplot, metrics in data.items():
                for metric, value in metrics.items():
                    # Append the latency to the metric name
                    new_metric = f"{metric}_{latency}"
                    dataset[subplot][new_metric] = value

        for latency, data in data_error_dict.items():
            for subplot, metrics in data.items():
                for metric, value in metrics.items():
                    # Append the latency to the metric name
                    new_metric = f"{metric}_{latency}"
                    dataset_error[subplot][new_metric] = value
        # Prepare attributes for stacked plot
        attributes_dict = {
            "subplot1": {
                "ylabel": "Throughput\n(Records/s)",
                "ylim": (0, 120000),
                "axhline": 100000,
            },
            "subplot2": {
                "ylabel": "Backpressure\n(%)",
                "ylim": (0, 100),
            },
        }

        print(dataset)

        # Generate stacked plot
        self.plotter.generate_stacked_plot_multiple_series_lazyass(
            dataset,
            dataset_error,
            attributes_dict=attributes_dict,
            xlabel="Operator Parallelism",
            filename="stacked_mean_with_backpressure.png",
        )

    def generate_box_plot_per_subtask(self, experiment_path):
        # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
        dfs = []
        for root, dirs, files in os.walk(experiment_path):
            for file in files:
                if file == "final_df.csv":
                    file_path = str(os.path.join(root, file))

                    df = pd.read_csv(file_path)

                    # Append the DataFrame to the list
                    dfs.append(df)

        # Concatenate all the DataFrames into a single DataFrame
        final_df = pd.concat(dfs)
        throughput_cols = [
            col
            for col in final_df.columns
            if "flink_taskmanager_job_task_numRecordsInPerSecond" in str(col)
        ]
        # Prepare data for boxplot
        boxplot_data = [final_df[col].dropna() for col in throughput_cols]
        labels = [col[1] for col in throughput_cols]

        # Create boxplot
        plt.boxplot(boxplot_data, labels=labels)
        plt.xlabel("Subtask")
        plt.ylabel("numRecordsInPerSecond")
        plt.title("Boxplot of numRecordsInPerSecond for each subtask")
        # plt.show()

    def generate_box_plot_per_parallelism(self, experiments_path):
        # Iterate through all subdirs of experiment_path and load final_df.csv of each subdir
        dfs = []
        for root, dirs, files in os.walk(experiments_path):
            for file in files:
                if file == "final_df.csv":
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)

                    # Group by 'Parallelism'. Timestamp column represents seconds. So we skip the first 60 seconds of each Paralaellism group
                    df = df.groupby("Parallelism").apply(lambda x: x.iloc[self.skip :])

                    # Get back to a normal DataFrame
                    df = df.reset_index(drop=True)

                    # Eval throughput
                    numRecordsInPerSecond_cols = [
                        col
                        for col in df.columns
                        if "flink_taskmanager_job_task_numRecordsInPerSecond"
                        in str(col)
                    ]

                    # Add a new column 'Sum' to the DataFrame which is the sum of 'numRecordsInPerSecond' across all subtasks
                    df["Throughput"] = df[numRecordsInPerSecond_cols].sum(axis=1)

                    # Only keep the columns 'Parallelism' and 'Throughput'
                    df = df[["Parallelism", "Throughput"]]

                    # Remove rows with Parallelism = 0
                    df = df[df["Parallelism"] != 0]
                    # Append the DataFrame to the list
                    dfs.append(df)
        # Get number of files
        num_runs = len(dfs)

        final_df = pd.concat(dfs)

        # Group by 'Parallelism' and use 'Parallelism' values as index
        final_df = final_df.groupby("Parallelism")
        final_df = final_df.apply(lambda x: x.reset_index(drop=True))
        # Drop the 'Parallelism' column
        final_df = final_df.drop(columns="Parallelism")
        # Convert the MultiIndex dataframe into a list of arrays
        boxplot_data = [
            group["Throughput"].values for _, group in final_df.groupby(level=0)
        ]
        labels = [name for name, _ in final_df.groupby(level=0)]

        fig, ax = plt.subplots()
        ax.boxplot(boxplot_data, labels=labels, showfliers=False, meanline=True)
        ax.set_xlabel("Operator Parallelism")
        ax.set_ylabel("Records per Second")

        # Add straight dotted line at y=100000
        ax.axhline(y=100000, color="r", linestyle="--", label="100000")

        # Decompose the path to get the operator name and the type of experiment
        experiment_path = experiments_path.split("/")
        experiment = experiment_path[-2]
        type = experiment_path[-1]
        # set title
        ax.set_title(f"{experiment} operator - {type}")
        # set subtitle
        # fig.suptitle(f"Experiment runs : {num_runs}", fontsize=12)
        # Save the plot
        output_path = os.path.join(experiments_path, f"{experiment}_{type}.png")
        fig.savefig(output_path)
        fig.show()

    # Load the mean_stderr.csv file and generate a boxplot
    def generate_box_for_means(self, exp_path):

        dfs = []
        for root, dirs, files in os.walk(exp_path):
            for file in files:
                if file == "mean_stderr.csv":
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    df = df.drop(0)
                    dfs.append(df)

        num_runs = len(dfs)
        final_df = pd.concat(dfs)

        final_df = final_df.groupby("Parallelism")
        final_df = final_df.apply(lambda x: x.reset_index(drop=True))
        final_df = final_df.drop(columns="Parallelism")
        boxplot_data = [
            group["Throughput"].values for _, group in final_df.groupby(level=0)
        ]

        # Eval mean Predictions per parallelism
        predictions = final_df["Predictions"].groupby(level=0).mean()
        # Interpolate missing values in the 'Predictions' series
        predictions = predictions.interpolate()

        # If you want to ensure that interpolated values are greater than 1, you can apply a condition
        # predictions = predictions.apply(lambda x: x if x > 1 else 1)

        labels = [name for name, _ in final_df.groupby(level=0)]
        formatter = FuncFormatter(self.__log.thousands_formatter)

        fig, ax = plt.subplots(figsize=(8, 6))
        ax.boxplot(
            boxplot_data,
            labels=labels,
            showfliers=False,
            meanline=True,
            whis=0,
        )

        ax.yaxis.set_major_formatter(formatter)
        ax.set_ylim(0, 120000)
        ax.set_xlabel("Operator Parallelism", fontsize=24)
        ax.set_ylabel("Throughput (records/s)", fontsize=24)
        plt.xticks(fontsize=20)
        ax.tick_params(axis="y", labelsize=20)
        offset_predictions = predictions * 0.90

        # Plot predictions
        ax.plot(
            predictions.index,
            offset_predictions.values,
            color="b",
            label="Prediction with error",
        )

        # Add straight dotted line at y=100000, This is the target objective for the experiment
        ax.axhline(y=100000, color="r", linestyle="--", label="Workload objective")

        # Add percentage error, between predictions and mean throughput
        percentage_error = (
            (offset_predictions - final_df["Throughput"].groupby(level=0).mean())
            / final_df["Throughput"].groupby(level=0).mean()
        ) * 100

        for x, y, error in zip(
            offset_predictions.index, offset_predictions.values, percentage_error
        ):
            ax.annotate(
                f"{error:.2f}%",
                (x, y),
                textcoords="offset points",
                xytext=(0, 20),
                ha="center",
                fontsize=16,
            )

        # Decompose the path to get the operator name and the type of experiment
        experiment_path = exp_path.split("/")
        experiment = experiment_path[-2]
        type = experiment_path[-1]
        title = f"{experiment} operator - {type}"
        # set title
        # ax.set_title(title)

        # Add a legend
        ax.legend(loc="lower right", fontsize=22)

        # set subtitle
        # fig.suptitle(f"Experiment runs : {num_runs}", fontsize=12)
        # Save the plot
        fig.tight_layout()
        output_path = os.path.join(exp_path, f"{experiment}_{type}_mean.png")
        fig.savefig(output_path)
        fig.show()

    def export(self):
        self.__log.info(f"Handling export for {self.base_path}")
        # Export data and evaluate mean and stderr for each experiment
        # for experiment in self.experiments:
        #     for type in self.types:
        #         experiment_path = os.path.join(self.base_path, experiment, type)
        #         # If path exists, but folder is empty, skip
        #         if os.path.exists(experiment_path):
        #             # Get subdirectories
        #             subdirs = [
        #                 f.path for f in os.scandir(experiment_path) if f.is_dir()
        #             ]
        #             for subdir in subdirs:
        #                 data: DataEval = DataEval(self.__log, subdir)
        #                 data.export()
        #                 # data.eval_mean_stderr()
        #                 # data.eval_summary_plot()
        #                 # data.eval_plot_with_checkpoints()
        #                 # data.eval_experiment_plot()
        #                 # data.eval_mean_with_backpressure()

        for experiment in self.experiments:
            for type in self.types:
                experiment_path = os.path.join(self.base_path, experiment, type)
                # If path exists, but folder is empty, skip
                if os.path.exists(experiment_path):
                    if len(os.listdir(experiment_path)) == 0:
                        continue
                    self.generate_box_for_means(experiment_path)
