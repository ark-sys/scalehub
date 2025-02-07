import json
import os
import re
from datetime import datetime

import networkx as nx
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from scripts.src.data.Plotter import Plotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import (
    DefaultKeys as Key,
    MAP_PIPELINE_DICT,
    JOIN_PIPELINE_DICT,
)
from scripts.utils.Logger import Logger


class DataEval:
    def __init__(self, log: Logger, exp_path: str):
        self.__log = log
        # Path to experiment folder
        self.exp_path = exp_path
        # Path to log file
        log_file = os.path.join(self.exp_path, "exp_log.json")

        # Create a folder for plots if it doesn't exist
        self.plots_path = os.path.join(self.exp_path, "plots")
        if not os.path.exists(self.plots_path):
            os.makedirs(self.plots_path)

        # Create a Plotter object
        self.plotter = Plotter(self.__log, plots_path=self.plots_path)

        # Parse configuration file for experiment
        self.conf: Config = Config(log, log_file)

        # Time to skip in seconds at the beginning and the end of a parallelism region
        self.start_skip = self.conf.get_int(Key.Experiment.output_skip_s.key)
        self.end_skip = 30

        if not hasattr(self, "final_df"):
            self.final_df = pd.read_csv(
                os.path.join(self.exp_path, "final_df.csv"),
                index_col=0,
                header=[0, 1, 2],
            )

    def __export_predictions(self) -> list[tuple[int, int, int, float]]:
        predictions = []
        transscale_log = os.path.join(self.exp_path, "transscale_log.txt")
        try:
            with open(transscale_log, "r") as file:
                log_content = file.read()

            # Define the regular expression pattern
            pattern = r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\] \[COMBO_CTRL\] Reconf: Scale (Up|Down) (.*) from par (\d+)([\s\S]*?)\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\] \[RES_MNGR\] Re-configuring PARALLELISM[\s\S].*\n.*Target Parallelism: (\d+)"
            # Extract the matches
            for match in re.finditer(pattern, log_content):
                # Extract the match
                current_parallelism = int(match.group(4))
                target_parallelism = int(match.group(6))
                prediction_block = match.group(5)
                operator = match.group(3)
                time = match.group(1)

                operator_name = self.conf.get(Key.Experiment.task_name.key)

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
        except FileNotFoundError:

            # self.__log.error("Failed to extract predictions from transscale log.")
            self.__log.error(
                "Failed to extract predictions from transscale log: File not found."
            )
        except Exception as e:
            self.__log.error(
                f"Failed to extract predictions from transscale log: {str(e)}"
            )
        finally:
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

        # Clean up the DataFrame: rows with parallelism > 0 that have 0 throughput are removed
        df_final = df_final[df_final["Throughput"] > 0]

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

        # Evaluate ylim for throughput, round up to nearest 10000
        ylim = (0, (numRecordsInPerSecond_df.max().max() // 10000 + 1) * 10000)

        # Prepare ylim dictionary
        ylim_dict = {
            "Throughput": ylim,
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
        jobname = self.conf.get(Key.Experiment.job_file.key)
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
        if self.conf.get(Key.Experiment.Chaos.enable.key) == "true":
            jitter = self.conf.get(Key.Experiment.Chaos.delay_jitter_ms.key)
            if int(jitter) > 0:
                title = f"{operator_name} - Latency and Jitter"
            else:
                title = f"{operator_name} - Latency"
        else:
            title = f"{operator_name} - No Latency"
        # eval max value for ylim from throughput. round up to nearest 50000
        y_val = dataset["Throughput"].max()
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
            ylim=(0, (dataset["Throughput"].max() // 50000 + 1) * 50000),
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
        experiment_name = self.conf.get(Key.Experiment.name.key)

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

        if self.conf.get(Key.Experiment.name.key) == "Join":
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
        elif self.conf.get(Key.Experiment.name.key) == "Map":
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
                f"Unknown experiment name: {self.conf.get(Key.Experiment.name.key)}"
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
        if self.conf.get(Key.Experiment.name.key) == "Join":
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

        elif self.conf.get(Key.Experiment.name.key) == "Map":
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
                f"Unknown experiment name: {self.conf.get(Key.Experiment.name.key)}"
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

    def eval_resource_plot(self):

        metrics_file_path = os.path.join(
            self.exp_path,
            "export",
            "flink_taskmanager_job_task_numRecordsInPerSecond_export.json",
        )

        json_data = []

        try:
            with open(metrics_file_path, "r") as file:
                for line in file:
                    json_data.append(json.loads(line))
        except Exception as e:
            self.__log.error("Failed to load metrics from json file.")
            return

        # Extract data and create DataFrame
        data = []
        for entry in json_data:
            pod = entry["metric"]["pod"]
            cpu_millis, mem_mb, _ = map(int, re.findall(r"\d+", pod))
            timestamps = entry["timestamps"]
            values = entry["values"]

            for ts, val in zip(timestamps, values):
                if val is not None:
                    data.append(
                        {
                            "cpu": cpu_millis / 1000,
                            "mem": mem_mb / 1024,
                            "timestamp": ts,
                            "throughput": val,
                        }
                    )

        df = pd.DataFrame(data)

        # Group by CPU and memory, and calculate mean throughput
        df_grouped = (
            df.groupby(["cpu", "mem"]).agg({"throughput": "mean"}).reset_index()
        )

        # Save the grouped DataFrame to a CSV file
        df_grouped.to_csv(os.path.join(self.exp_path, "resource_data.csv"))
        # Call the plotter to generate the 3D plot
        self.plotter.generate_3d_plot(
            df_grouped["cpu"],
            df_grouped["mem"],
            df_grouped["throughput"],
            title="Throughput vs CPU and Memory",
            xlabel="CPU (cores)",
            ylabel="Memory (GB)",
            zlabel="Throughput (Records/s)",
            filename="resource_plot.png",
        )
