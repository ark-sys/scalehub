import os
import re

import pandas as pd

from scripts.src.data.Plotter import Plotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class GroupedDataEval:
    def __init__(self, log: Logger, exp_path: str):
        self.__log = log
        self.t = Tools(log)
        self.plotter = Plotter(self.__log, plots_path=exp_path)
        self.base_path = exp_path

    def __load_mean_stderr_files(self):
        dfs = []
        try:
            for root, _, files in os.walk(self.base_path):
                for file in files:
                    if file == "mean_stderr.csv":
                        file_path = os.path.join(root, file)

                        data = pd.read_csv(file_path)

                        dfs.append(data)
            return dfs
        except Exception as e:
            self.__log.error(f"Error loading mean_stderr files: {e}")
            raise e

    def __load_final_df_files(self):
        dfs = {}
        try:
            # Get the immediate subdirectories
            immediate_subdirs = [
                d
                for d in os.listdir(self.base_path)
                if os.path.isdir(os.path.join(self.base_path, d))
            ]

            # Process each immediate subdirectory
            for subdir in immediate_subdirs:
                subdir_path = os.path.join(self.base_path, subdir)
                final_df_path = os.path.join(subdir_path, "final_df.csv")

                # Check if final_df.csv exists in this subdirectory
                if os.path.isfile(final_df_path):
                    data = pd.read_csv(final_df_path)
                    dfs[subdir] = data
                    self.__log.info(f"Loaded {subdir} at {final_df_path}")

        except Exception as e:
            self.__log.error(f"Error loading final_df files from {self.base_path}: {e}")
            raise

        return dfs

    @staticmethod
    def __aggregate_data(dfs):
        final_df = pd.concat(dfs)
        final_df = (
            final_df.groupby("Parallelism")
            .agg(
                Throughput_min=("Throughput", "min"),
                Throughput_max=("Throughput", "max"),
                Throughput_mean=("Throughput", "mean"),
                Throughput_stderr=("ThroughputStdErr", "mean"),
            )
            .reset_index()
        )
        return final_df

    def generate_box_plot(self):
        dfs = self.__load_mean_stderr_files()
        final_df = self.__aggregate_data(dfs)
        final_df.to_csv(os.path.join(self.base_path, "final_df.csv"), index=False)

        boxplot_data = [
            [
                group["Throughput_min"].values[0],
                group["Throughput_mean"].values[0],
                group["Throughput_max"].values[0],
            ]
            for _, group in final_df.groupby("Parallelism")
        ]
        labels = [str(name) for name, _ in final_df.groupby("Parallelism")]
        workload_objective = sum(
            int(gen["num_sensors"])
            for gen in Config(
                self.__log, os.path.join(self.base_path, "1", "exp_log.json")
            ).get(Key.Experiment.Generators.generators.key)
        )
        ylim_val = (
            max(final_df["Throughput_max"].max(), workload_objective) // 100000 + 1
        ) * 100000
        self.plotter.generate_whisker_plot(
            boxplot_data=boxplot_data,
            labels=labels,
            workload_objective=workload_objective,
            ylim_val=ylim_val,
            # comment=Config(
            #     self.__log, os.path.join(self.base_path, "1", "exp_log.json")
            # ).get(Key.Experiment.comment.key),
        )

    def generate_box_plot_multi_exp(self):
        """
        Generate a box plot comparing multiple experiments in a multi_exp folder.
        Each experiment (subfolder) contains a final_df.csv with min, max, mean throughput.
        """
        # Load all final_df files from subfolders
        final_dfs = self.__load_final_df_files()

        if not final_dfs:
            self.__log.error("No final_df.csv files found for box plot")
            return

        # Process data for box plot
        boxplot_data = []
        labels = []

        # Sort the keys to ensure consistent ordering in the plot
        sorted_keys = sorted(final_dfs.keys())
        # If key is in the format "x" where x is a number, filter it

        self.__log.info(
            f"Generating box plot for {len(final_dfs)} experiments: {final_dfs}"
        )

        for exp_name in sorted_keys:
            df = final_dfs[exp_name]

            # Check if the required columns are present
            required_cols = ["Throughput_min", "Throughput_max", "Throughput_mean"]
            if not all(col in df.columns for col in required_cols):
                self.__log.warning(f"Missing required columns in {exp_name}, skipping")
                continue

            # Extract the min, max, mean values for the box plot
            # Each "box" consists of [min, mean, max] values
            values = [
                df["Throughput_min"].values[0],
                df["Throughput_mean"].values[0],
                df["Throughput_max"].values[0],
            ]

            boxplot_data.append(values)

            # # Clean the label by removing the "p2_" prefix
            # clean_label = exp_name.replace("p2_", "")

            # Parse the experiment name format "X_tm_Y_ts_per_tm"
            # where X is the number of task managers and Y is the number of task slots per task manager
            tm_match = re.match(r"(\d+)_tm_(\d+)_ts_per_tm", exp_name)
            if tm_match:
                num_tm = int(tm_match.group(1))
                num_ts_per_tm = int(tm_match.group(2))
                clean_label = f"{num_tm} TM,\n{num_ts_per_tm} TS/TM"
            else:
                clean_label = (
                    exp_name  # Default to original name if pattern doesn't match
                )

            labels.append(clean_label)

        if not boxplot_data:
            self.__log.error("No valid data found for box plot")
            return

        boxplot_data = [boxplot_data[1], boxplot_data[0], boxplot_data[2]]
        labels = [labels[1], labels[0], labels[2]]

        for i, label in enumerate(labels):
            exp_index = f"({'i' * (i + 1)})"
            # Prepend the experiment index to the label
            labels[i] = f"{exp_index} {label}"

        # Determine appropriate y-axis limit
        max_throughput = max(max(box) for box in boxplot_data)

        # Generate the box plot
        self.plotter.generate_whisker_plot(
            boxplot_data=boxplot_data,
            labels=labels,
            ylim_val=400000,
            xlabel="TaskManager Configuration",
            ylabel="Throughput (records/s)",
            # comment="Comparison of Experiment Throughputs",
            filename="multi_experiment_box_plot.png",
            workload_objective=350000,
        )

        self.__log.info(
            f"Generated multi-experiment box plot at {self.plotter.plots_path}"
        )

    def _get_multi_exp_data(self):
        multi_exp_data = {}
        for folder in os.listdir(self.base_path):
            folder_path = os.path.join(self.base_path, folder)
            final_df_path = os.path.join(folder_path, "final_df.csv")
            exp_log_path = os.path.join(folder_path, "1", "exp_log.json")
            if os.path.exists(final_df_path) and os.path.exists(exp_log_path):
                config = Config(self.__log, exp_log_path)
                final_df = pd.read_csv(final_df_path)
                multi_exp_data[folder] = (config, final_df)
        return multi_exp_data

    def generate_multi_exp_plot(self, single_node=True):
        # Centralized style definition for machine types
        machine_styles = {
            "BM": {"marker": "o", "color": "#1f77b4"},  # Blue circle
            "VM-L": {"marker": "s", "color": "#d62728"},  # Red square
            "VM-S": {"marker": "D", "color": "#2ca02c"},  # Green diamond
            "RPi": {"marker": "^", "color": "#9467bd"},  # Purple triangle
        }

        experiment_data = self._get_multi_exp_data()
        plot_data = {}
        error_data = {}
        custom_markers = {}
        custom_colors = {}
        custom_legends = {}
        custom_point_colors = {}

        # Process each experiment
        for exp_name, (config, final_df) in experiment_data.items():
            # Extract throughput data
            new_df = final_df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)

            # Extract standard error data if available
            stderr_df = None
            if "Throughput_stderr" in final_df.columns:
                stderr_df = final_df.loc[:, ["Parallelism", "Throughput_stderr"]].copy()
                stderr_df.rename(columns={"Throughput_stderr": "StdErr"}, inplace=True)

            # Determine if this is a mixed node experiment
            is_mixed_node = "mix_node" in exp_name.lower()

            # WORKAROUND: Special handling for the problematic VM-S mixed node experiment (mix_node_vms_pico)
            if is_mixed_node and "vms" in exp_name.lower():
                self.__log.warning(
                    f"Applying workaround for VM-S mixed node experiment: {exp_name}"
                )
                if len(new_df) > 1:
                    new_df = new_df.drop(index=1).reset_index(drop=True)
                    if stderr_df is not None:
                        stderr_df = stderr_df.drop(index=1).reset_index(drop=True)
                    self.__log.info(
                        f"Removed problematic second row from {exp_name} data"
                    )

            # Format index based on an experiment type
            if single_node:
                new_df.set_index("Parallelism", inplace=True)
                if stderr_df is not None:
                    stderr_df.set_index("Parallelism", inplace=True)
            else:
                new_df.reset_index(drop=True, inplace=True)
                new_df.index = new_df.index + 1
                if stderr_df is not None:
                    stderr_df.reset_index(drop=True, inplace=True)
                    stderr_df.index = stderr_df.index + 1

            # Export data CSV
            new_df.to_csv(
                os.path.join(self.base_path, f"{exp_name}_plot_data.csv"), index=True
            )

            # Parse experiment name to identify machine types
            machine_types = []
            display_name = ""
            if is_mixed_node:
                parts = exp_name.lower().split("_")
                if len(parts) >= 4:
                    first_type = self._get_machine_type(parts[2])
                    second_type = self._get_machine_type(parts[3])
                    machine_types = [first_type, second_type]
                    display_name = f"{first_type} + {second_type}"
            else:
                machine_type = self._get_machine_type(exp_name)
                machine_types = [machine_type]
                display_name = machine_type

            # Store data
            plot_data[display_name] = new_df["Throughput"]
            if stderr_df is not None:
                error_data[display_name] = stderr_df["StdErr"]

            # Set visualization properties based on machine types
            if is_mixed_node and len(machine_types) == 2:
                # For mixed nodes, create point-specific markers based on index
                point_markers = {}
                point_colors = {}

                for idx in new_df.index:
                    # For multi_node plots, maintain consistent marker and color mapping
                    if not single_node:
                        # Always use the machine type's assigned marker and color
                        marker_type = machine_types[0] if idx == 1 else machine_types[1]
                    else:
                        # For single_node, follow the existing pattern
                        marker_type = machine_types[0] if idx == 1 else machine_types[1]

                    point_markers[idx] = machine_styles.get(
                        marker_type, {"marker": "o"}
                    )["marker"]

                    # Always use the correct machine type color for its marker
                    point_colors[idx] = machine_styles.get(
                        marker_type, {"color": "#000000"}
                    )["color"]

                custom_markers[display_name] = point_markers
                custom_point_colors[display_name] = point_colors

                # Use the FIRST machine type's color for the connecting line (for grayscale this will be overridden)
                custom_colors[display_name] = machine_styles.get(
                    machine_types[0], {"color": "#000000"}
                )["color"]

                # Set custom legend to show both markers with their proper colors
                custom_legends[display_name] = {
                    "markers": [
                        machine_styles.get(machine_types[0], {"marker": "o"})["marker"],
                        machine_styles.get(machine_types[1], {"marker": "s"})["marker"],
                    ],
                    "colors": [
                        machine_styles.get(machine_types[0], {"color": "#000000"})[
                            "color"
                        ],
                        machine_styles.get(machine_types[1], {"color": "#000000"})[
                            "color"
                        ],
                    ],
                    "label": display_name,
                }
                # Ensure the display_name is also added to custom_colors
                if display_name not in custom_colors:
                    custom_colors[display_name] = machine_styles.get(
                        machine_types[0], {"color": "#000000"}
                    )["color"]
            else:
                # For single machine type, use consistent marker and color
                machine_type = machine_types[0]
                custom_markers[display_name] = machine_styles.get(
                    machine_type, {"marker": "o"}
                )["marker"]
                custom_colors[display_name] = machine_styles.get(
                    machine_type, {"color": "#000000"}
                )["color"]

        # Set up plot parameters
        ylabels_dict = {"Throughput": "Throughput (records/s)"}

        # Generate the plot
        self.plotter.generate_single_frame_multiple_series_plot(
            ax1_data=plot_data,
            ax1_error_data=error_data if error_data else None,
            xlabel="Number of TaskManagers" if single_node else "Number of Machines",
            ylabels_dict=ylabels_dict,
            filename=os.path.join(
                self.base_path,
                "single_node_throughput_new.png"
                if single_node
                else "multi_node_throughput_new.png",
            ),
            ylim=(0, 400000),
            axhline=350000,
            zoom_region=(0, 3, 0, 40000) if single_node else None,
            custom_markers=custom_markers,
            custom_colors=custom_colors,
            custom_point_colors=custom_point_colors,
            custom_legends=custom_legends,
        )

    @staticmethod
    def _get_machine_type(name):
        """Extract standardized machine name from a string."""
        name = name.lower()
        if "bm" in name:
            return "BM"
        elif "vml" in name or "vm-l" in name:
            return "VM-L"
        elif "vms" in name or "vm-s" in name:
            return "VM-S"
        elif "pico" in name:
            return "RPi"
        else:
            # If no match, return a cleaned version of the original name
            return name.replace("single_node_", "").replace("_", " ").title()

    def process_resource_data(self):
        resource_data = {}
        subdirs = [d for d in os.listdir(self.base_path) if "flink" in d]
        for subdir in subdirs:
            subdir_path = os.path.join(self.base_path, subdir)
            final_df_path = os.path.join(subdir_path, "final_df.csv")
            try:
                df = pd.read_csv(final_df_path)
                throughput = df["Throughput_mean"].values[0]
                match = re.search(r"flink-(\d+)m-(\d+)(-(.*))?", subdir)
                cpu, mem = int(match.group(1)) // 1000, int(match.group(2)) // 1024
                resource_data[(cpu, mem)] = throughput
            except FileNotFoundError:
                self.__log.error(
                    f"final_df.csv not found in {subdir_path}. Skipping directory."
                )
        return resource_data

    def generate_resource_plot(self):
        resource_data = self.process_resource_data()
        final_df = pd.DataFrame(
            [
                {"cpu": cpu, "mem": mem, "throughput": throughput}
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df.to_csv(os.path.join(self.base_path, "resource_data.csv"), index=False)
        self.plotter.generate_3d_plot(
            final_df["cpu"],
            final_df["mem"],
            final_df["throughput"],
            "",
            "CPU (cores)",
            "Memory (GB)",
            "Throughput (Records/s)",
            "resource_plot_multi_run.png",
        )

    def generate_resource_core_info(self):
        nodes_config = {
            "bm": {"cpu": 52, "mem": 386},
            "vml": {"cpu": 8, "mem": 32},
            "vms": {"cpu": 2, "mem": 8},
            "pico": {"cpu": 4, "mem": 4},
        }
        resource_data = self.process_resource_data()
        final_df = pd.DataFrame(
            [
                {"cpu": cpu, "mem": mem, "throughput": throughput}
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df["tpt_per_core"] = final_df["throughput"] / final_df["cpu"]
        node_name = os.path.basename(self.base_path).split("_")[2]
        node_config = nodes_config[node_name]
        final_df["inst_full"] = final_df.apply(
            lambda row: min(
                node_config["cpu"] // row["cpu"], node_config["mem"] // row["mem"]
            ),
            axis=1,
        )
        final_df["exp_tpt_full"] = final_df["throughput"] * final_df["inst_full"]

        # Sort by cpu,mem
        final_df.sort_values(by=["cpu", "mem"], inplace=True)

        final_df.to_csv(
            os.path.join(self.base_path, "resource_core_info.csv"), index=False
        )
        best_row = final_df.loc[final_df["exp_tpt_full"].idxmax()]
        latex_data = final_df.to_latex(
            index=False,
            float_format="%.2f",
            escape=True,
            caption=f"Throughput evaluation for {node_name.upper()}",
        )
        best_row_string = f"Best throughput per core of {round(best_row['throughput'], 2)} is achieved with {int(best_row['cpu'])} CPUs, {round(best_row['mem'], 1)} GB of memory. With this configuration, a {node_name} machine can host {int(best_row['inst_full'])} instances, with an expected throughput of {round(best_row['exp_tpt_full'], 2)}."
        latex_data = latex_data.replace(
            "\\end{tabular}", "\\end{tabular}\n" f"\\textbf{{{best_row_string}}}\n"
        )
        with open(os.path.join(self.base_path, "resource_core_info.tex"), "w") as file:
            file.write(latex_data)
