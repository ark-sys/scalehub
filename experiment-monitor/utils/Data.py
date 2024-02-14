import configparser as cp
import os
import re
from datetime import datetime
from typing import Any

import pandas as pd
import pytz
from matplotlib import pyplot as plt
from matplotlib.legend_handler import HandlerTuple

from .Config import Config
from .Defaults import DefaultKeys as Key
from .Logger import Logger


class ExperimentData:
    BASE_TIMESERIES = "flink_operator"

    def __init__(self, log: Logger, exp_path: str, config: Config, force: bool = False):
        self.__log = log
        # Path to experiment folder
        self.exp_path = exp_path
        # Path to log file
        self.log_file = os.path.join(self.exp_path, "exp_log.txt")
        # Path to transscale log file
        self.transscale_log = os.path.join(self.exp_path, "transscale_log.txt")

        # Parse configuration file for experiment
        self.cp = cp.ConfigParser()
        self.config: Config = config
        self.force = force

        # Parse log file for timestamps
        self.start_ts, self.end_ts = self.__get_timestamps_from_log(self.log_file)

        # Time to skip in seconds at the beginning and the end of a parallelism region
        self.start_skip = 30
        self.end_skip = 30
        # VictoriaMetrics database url
        self.db_url = "victoria-metrics-single-server.default.svc.cluster.local:8428"

        # Placeholders for throughput_in, throughput_out, parallelism, backpressure and busyness timeseries
        self.tpo_timeseries = ""
        self.tpi_timeseries = ""
        self.par_timeseries = ""
        self.bpr_timeseries = ""
        self.bus_timeseries = ""

    def __get_timestamps_from_log(self, log_path: str) -> tuple[int, int]:
        # Parse log file for experiment info
        with open(log_path, "r") as log_file:
            logs = log_file.read()
        # Extract start and end timestamps from log file
        start_ts = int(re.search(r"Experiment start at : (\d+)", logs).group(1))
        end_ts = int(re.search(r"Experiment end at : (\d+)", logs).group(1))

        # Check that timestamps are valid
        if start_ts is None or end_ts is None:
            self.__log.error("Failed to parse timestamps from log file.")
            exit(1)

        # Return timestamps
        return start_ts, end_ts

    def export_timeseries(
        self,
        time_series_name: str,
        format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        output_file = os.path.join(self.exp_path, f"{time_series_name}_export.csv")

        # If file exists and force is not set, skip export, otherwise export data
        if os.path.exists(output_file) and not self.force:
            self.__log.info(
                f"Skipping export of {time_series_name} timeseries: file exists."
            )
            return output_file
        else:
            # VictoriaMetrics export CSV api
            api_url = f"http://{self.db_url}/api/v1/export/csv"
            params = {
                "format": format_labels,
                "match": time_series_name,
                "start": self.start_ts,
                "end": self.end_ts,
            }
            import requests

            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                with open(output_file, "wb") as file:
                    file.write(response.content)
                self.__log.info(f"Data exported to {output_file}")
                return output_file
            else:
                self.__log.error(f"Error exporting data: {response.text}")

    def export_experiment_data(self):
        # Retrieve operator name from config file
        operator_name = self.config.get(Key.Experiment.task_name)

        # Build timeseries names with operator name
        self.tpo_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_throughput_out"
        self.tpi_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_throughput_in"
        self.par_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_parallelism"
        self.bpr_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_backpressure"
        self.bus_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_busyness"

        # Export timeseries from database to csv file
        tpo_timeseries_path = self.export_timeseries(
            time_series_name=self.tpo_timeseries,
        )
        par_timeseries_path = self.export_timeseries(
            time_series_name=self.par_timeseries,
        )

        tpi_timeseries_path = self.export_timeseries(
            time_series_name=self.tpi_timeseries,
        )
        bpr_timeseries_path = self.export_timeseries(
            time_series_name=self.bpr_timeseries,
        )
        bus_timeseries_path = self.export_timeseries(
            time_series_name=self.bus_timeseries,
        )

        if (
            tpo_timeseries_path is None
            or par_timeseries_path is None
            or tpi_timeseries_path is None
            or bpr_timeseries_path is None
            or bus_timeseries_path is None
        ):
            self.__log.error(
                "Failed to process path for timeseries: could not join data. Something went wrong during export."
            )
            exit(1)
        else:
            df1 = pd.read_csv(tpo_timeseries_path).dropna()
            df2 = pd.read_csv(par_timeseries_path).dropna()
            df3 = pd.read_csv(tpi_timeseries_path).dropna()
            df4 = pd.read_csv(bpr_timeseries_path).dropna()
            df5 = pd.read_csv(bus_timeseries_path).dropna()

            # Check if any of the exported CSVs are empty
            if df1.empty or df2.empty or df3.empty or df4.empty or df5.empty:
                self.__log.error(
                    "One or more exported CSVs appear to be empty. Failed to join."
                )
                exit(1)
            else:
                # Join columns in one table and drop labels

                # Give the code below
                # df2['Parallelism'] = df2['Parallelism'].fillna(method='ffill')

                # Join columns in one table and drop labels
                df_merged = df2.merge(df1).drop(
                    [self.par_timeseries, self.tpo_timeseries],
                    axis=1,
                )

                df3 = pd.read_csv(tpi_timeseries_path).dropna()
                # Add input metrics to output csv
                df_merged["Throughput_IN"] = df3.iloc[:, 2]
                # Provide column naming
                df_merged.columns = [
                    "Time",
                    "Parallelism",
                    "Throughput_OUT",
                    "Throughput_IN",
                ]

                # Check if transscale log exists
                if os.path.exists(self.transscale_log):
                    # Transscale log found, try to extract predictions
                    predictions = self.__export_predictions()
                    if predictions:
                        # Create a dataframe from the predictions
                        predictions_df = pd.DataFrame(
                            predictions,
                            columns=[
                                "Time",
                                "Current_Parallelism",
                                "Parallelism",
                                "Predicted_Throughput",
                            ],
                        )

                        predictions_df["Time"] = (
                            predictions_df["Time"] - predictions_df["Time"].min()
                        )
                        # Sort the dataframe by 'Time'
                        df_merged.sort_values("Time", inplace=True)

                        # Subtract minimum time from all time values to start from 0
                        df_merged["Time"] = df_merged["Time"] - df_merged["Time"].min()

                        df_merged["Parallelism"] = df_merged["Parallelism"].astype(
                            "int64"
                        )
                        dataset = pd.merge_asof(
                            df_merged,
                            predictions_df,
                            by="Parallelism",
                            on="Time",
                            direction="nearest",
                        )
                        dataset["Predicted_Throughput"] = dataset[
                            "Predicted_Throughput"
                        ].ffill()

                output_file = os.path.join(self.exp_path, "joined_output.csv")
                df_merged.dropna().to_csv(output_file, index=False)

    def __export_predictions(self) -> list[tuple[int, int, int, float]]:
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

            operator_name = self.config.get(Key.Experiment.task_name)

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

        return predictions

    #
    # # This function takes a join.csv file, which contains throughput and parallelism metric from an experiment And
    # # evaluates mean and stdev throughput for each parallelism. If predictions are found for experiment,
    # # they are added to output file named stats.csv
    # def eval_stats(self, skip_duration):
    #     def extract_predictions(log_path) -> dict[Any, list[Any]]:
    #         predictions = {}
    #         target_pattern = r"((?:.*\n){20}.*Re-configuring PARALLELISM\n.*Current Parallelism: (\d+)\n.*Target Parallelism: (\d+))"
    #
    #         with open(log_path, "r") as log_file:
    #             logs = log_file.read()
    #
    #             # Extract strings from match to match-15 lines
    #             match_blocks = re.findall(target_pattern, logs)
    #             for match in match_blocks:
    #                 block = match[0].strip()
    #                 target_parallelism = match[2]
    #                 tpt_pattern = rf"\bpar, transp: {target_parallelism}, 1 target tput: \d+ new_tput %: (\d+\.\d+)"
    #                 prediction = re.search(tpt_pattern, block)
    #                 if prediction:
    #                     prediction_value = float(prediction.group(1))
    #
    #                     # Initialize an empty list if target_parallelism doesn't exist in predictions
    #                     if target_parallelism not in predictions:
    #                         predictions[target_parallelism] = []
    #
    #                     # Calculate region
    #                     region = len(predictions[target_parallelism]) + 1
    #
    #                     predictions[target_parallelism].append(
    #                         (region, prediction_value)
    #                     )
    #             # Return list of predictions
    #             return predictions
    #
    #     joined_output_path = os.path.join(self.exp_path, "joined_output.csv")
    #     try:
    #         # Read the CSV file with comma delimiter
    #         df = pd.read_csv(joined_output_path, delimiter=",")
    #
    #         # Convert 'Time' column to datetime
    #         df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)
    #
    #         # Add UTC+2 to datetime
    #         df["Time"] = df["Time"].dt.tz_convert(pytz.timezone("Europe/Paris"))
    #
    #         # Create a temporary 'region' column
    #         df["temp_region"] = (df["Parallelism"].diff(1) != 0).astype("int").cumsum()
    #
    #         # Filter out rows where the count of 'Parallelism' in the 'temp_region' is less than 6
    #         df = df[df.groupby("temp_region")["Parallelism"].transform("count") > 5]
    #
    #         # Drop the temporary 'region' column
    #         df = df.drop(columns=["temp_region"])
    #
    #         # Now create the actual 'region' column on the filtered DataFrame
    #         df["region"] = (df["Parallelism"].diff(1) != 0).astype("int").cumsum()
    #
    #         # Now you can group by 'Parallelism' and 'region'
    #         grouped = df.groupby(["Parallelism", "region"])
    #
    #         # For each group, skip the first X seconds
    #         filtered_df = grouped.apply(
    #             lambda x: x[
    #                 x["Time"] >= x["Time"].min() + pd.Timedelta(seconds=skip_duration)
    #                 ]
    #         ).reset_index(drop=True)
    #
    #         # Make sure that Parallelism values are integers and not floats
    #         filtered_df["Parallelism"] = filtered_df["Parallelism"].astype(int)
    #
    #         # Calculate mean throughput and standard error
    #         stats_tpo = filtered_df.groupby(["Parallelism", "region"])[
    #             "Throughput_OUT"
    #         ].agg(["mean", "std"])
    #         stats_tpi = filtered_df.groupby(["Parallelism", "region"])[
    #             "Throughput_IN"
    #         ].agg(["mean", "std"])
    #         # Combine the mean and std for 'Throughput' and 'Input' into a single DataFrame
    #         stats = pd.concat(
    #             [stats_tpo, stats_tpi], axis=1, keys=["Throughput_OUT", "Throughput_IN"]
    #         )
    #         stats["region"] = stats.groupby("Parallelism").cumcount() + 1
    #         stats.index = stats.index.droplevel("region")
    #         stats.set_index("region", append=True, inplace=True)
    #         # Check if transscale log exists
    #         if os.path.exists(self.transscale_log):
    #             # Transscale log found, try to extract predictions
    #             self.__log.info("Transscale log found. Preparing predictions export.")
    #             predictions = extract_predictions(self.transscale_log)
    #
    #             # Create a new DataFrame from the dictionary
    #             pred_df = pd.DataFrame(
    #                 [(int(k), *i) for k, v in predictions.items() for i in v],
    #                 columns=["Parallelism", "region", "Predictions"],
    #             )
    #             # # Set 'Parallelism' and 'region' as index in the new DataFrame
    #             pred_df.set_index(["Parallelism", "region"], inplace=True)
    #             stats = pd.concat([stats, pred_df], axis=1)
    #
    #         # Save stats to a CSV file in the same path as the input filename
    #         output_path = os.path.join(self.exp_path, "stats.csv")
    #         stats.to_csv(output_path, index=True)
    #         self.__log.info(f"Stats saved to: {output_path}")
    #         return stats, output_path
    #     except FileNotFoundError:
    #         raise Exception(f"File not found: {joined_output_path}")
    #     except pd.errors.EmptyDataError:
    #         raise Exception(f"No data found to plot in file: {joined_output_path}")
    #
    # def eval_plot(self, stats):
    #     fig, ax = plt.subplots()  # Create a new figure and axes for plot
    #
    #     # Retrieve other experiment information from config file
    #     job_name = self.config.get(Key.Experiment.job_file)
    #     latency_enabled = self.config.get_bool(Key.Experiment.Chaos.enable)
    #     latency_delay = self.config.get_int(Key.Experiment.Chaos.delay_latency_ms)
    #     latency_jitter = self.config.get_int(Key.Experiment.Chaos.delay_jitter_ms)
    #     latency_correlation = self.config.get_float(
    #         Key.Experiment.Chaos.delay_correlation
    #     )
    #
    #     # Parse load_generators from config file: sum num_sensors from all generators and evaluate average interval
    #     num_sensors_sum, avg_interval_ms = 0, 0
    #     for generator in self.config.get(Key.Experiment.Generators.generators):
    #         num_sensors_sum += int(generator["num_sensors"])
    #         avg_interval_ms += int(generator["interval_ms"])
    #     avg_interval_ms = avg_interval_ms / len(
    #         self.config.get(Key.Experiment.Generators.generators)
    #     )
    #
    #     # TODO Change this in the future to properly show throughput in case of scale down
    #     filtered_stats = stats[stats.index.get_level_values("region") == 1]
    #
    #     # Extract the data from the MultiIndex DataFrame
    #     parallelism_values = filtered_stats.index.get_level_values("Parallelism")
    #     throughput_mean = filtered_stats[("Throughput_IN", "mean")]
    #     throughput_std = filtered_stats[("Throughput_IN", "std")]
    #     predictions = (
    #         filtered_stats["Predictions"]
    #         if "Predictions" in filtered_stats.columns
    #         else None
    #     )
    #
    #     ax.errorbar(
    #         parallelism_values,
    #         throughput_mean,
    #         yerr=throughput_std,
    #         fmt="o",
    #         linestyle="-",
    #         color="b",
    #         capsize=4,
    #         label="Throughput In",
    #     )
    #
    #     # Check if 'Predictions' column exists and that it contains more than one value.
    #     # If so, add dashed line with its values to plot
    #     if predictions is not None and len(predictions) > 1:
    #         ax.plot(
    #             parallelism_values,
    #             predictions,
    #             linestyle="--",
    #             marker="o",
    #             color="r",
    #             label="Predictions",
    #         )
    #         # Calculate percentage error and add it to the plot
    #         percentage_error = ((predictions - throughput_mean) / throughput_mean) * 100
    #         for x, y, error in zip(parallelism_values, predictions, percentage_error):
    #             ax.annotate(
    #                 f"{error:.2f}%",
    #                 (x, y),
    #                 textcoords="offset points",
    #                 xytext=(0, 10),
    #                 ha="center",
    #             )
    #
    #     # Job name is in the format: my<operator_name>-transscale-<type>-all.jar
    #     # We want to extract only the operator name and the type (which can be null).
    #     # Example: mymap-transscale-all.jar -> Map
    #     # Example: myjoin-transscale-all.jar -> Join (key-key)
    #     # Example: myjoin-transscale-kv-all.jar -> Join (key-value)
    #     # Example: myjoin-transscale-vv-all.jar -> Join (value-value)
    #     # Set plot title based on job name
    #     plot_title = ""
    #     if "map" in job_name:
    #         plot_title = f"Map"
    #     elif "join" in job_name:
    #         if "kv" in job_name:
    #             plot_title = f"Join (key-value)"
    #         elif "vv" in job_name:
    #             plot_title = f"Join (value-value)"
    #         else:
    #             plot_title = f"Join (key-key)"
    #
    #     ax.set_title(plot_title)
    #     ax.set(
    #         xlabel="Parallelism",
    #         ylabel="Throughput (records/s)",
    #         xticks=list(range(1, len(parallelism_values) + 1)),
    #     )
    #
    #     # Set legend for lines on lower right corner
    #     ax.legend(loc="lower right", handler_map={tuple: HandlerTuple(ndivide=None)})
    #
    #     # Add text to upper left corner with latency information if enabled, otherwise add text with no latency
    #     if latency_enabled:
    #         values_text = f"Latency: {latency_delay}\nJitter: {latency_jitter}\nCorrelation: {latency_correlation}"
    #         ax.text(
    #             0.02,
    #             0.95,
    #             values_text,
    #             transform=ax.transAxes,
    #             fontsize=10,
    #             verticalalignment="top",
    #         )
    #     else:
    #         ax.text(
    #             0.02,
    #             0.95,
    #             "No latency",
    #             transform=ax.transAxes,
    #             fontsize=10,
    #             verticalalignment="top",
    #         )
    #
    #     # Set the output filename
    #     latency_value_filename = "latency" if latency_enabled else "nolatency"
    #     filename = f"{job_name.split('.')[0]}_{latency_value_filename}.png"
    #
    #     # Export plot to experiment path
    #     output_filename = os.path.join(self.exp_path, filename)
    #     fig.savefig(output_filename)
    #     self.__log.info(f"Plot saved to: {output_filename}")
    #
    # def eval_summary_plot(self):
    #
    #     joined_data_file = f"{self.exp_path}/joined_output.csv"
    #
    #     # Read joined_output.csv file
    #     dataset = pd.read_csv(joined_data_file)
    #
    #     # # Calculate the rolling mean and standard deviation
    #     # rolling_mean = dataset['Parallelism'].rolling(window=5).mean()
    #     # rolling_std = dataset['Parallelism'].rolling(window=5).std()
    #     #
    #     # # Identify outliers
    #     # outliers = (dataset['Parallelism'] < (rolling_mean - 2 * rolling_std)) | (
    #     #         dataset['Parallelism'] > (rolling_mean + 2 * rolling_std))
    #     #
    #     # # Replace outliers with NaN
    #     # dataset.loc[outliers, 'Parallelism'] = None
    #
    #     # Plot timeseries
    #     fig, ax1 = plt.subplots()
    #
    #     color = 'tab:blue'
    #     ax1.set_xlabel('time (s)')
    #     ax1.set_ylabel('Throughput', color=color)
    #     ax1.plot(dataset['Time'], dataset['Throughput_IN'], color=color, linestyle='-')
    #     ax1.tick_params(axis='y', labelcolor=color)
    #     ax1.grid(axis='y', linestyle='--', linewidth=0.5)
    #
    #     ax2 = ax1.twinx()
    #     color = 'tab:gray'
    #     ax2.set_ylabel('parallelism', color=color)
    #     ax2.plot(dataset['Time'], dataset['Parallelism'], color=color)
    #     ax2.tick_params(axis='y', labelcolor=color)
    #     ax2.set_ylim(0, 25)
    #
    #     color = 'tab:red'
    #     # ax1.set_ylabel('predicted throughput', color=color)
    #     ax1.plot(dataset['Time'], dataset['Predicted_Throughput'], color=color, linestyle='--')
    #
    #     # Save the plot to a file
    #     plot_file = f"{self.exp_path}/big_plot.png"
    #     plt.savefig(plot_file)
    #     self.__log.info(f"Plot saved to: {plot_file}")
