import os
import re
from typing import Dict, Any, List

import pandas as pd

from scripts.src.data.base.data_processor import DataProcessor
from scripts.src.data.implementations.default_plotter import DefaultPlotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Tools import Tools


class GroupedExperimentProcessor(DataProcessor):
    """Processes data for multiple grouped experiments."""

    def __init__(self, logger, config: Config, exp_path: str):
        super().__init__(logger, exp_path)
        self.config = config
        self.tools = Tools(logger)
        self._setup_components()

    def _setup_components(self) -> None:
        """Initialize required components."""
        plots_path = self.exp_path / "plots"
        plots_path.mkdir(exist_ok=True)
        self.plotter = DefaultPlotter(self.logger, str(plots_path))

    def process(
        self,
        dry_run: bool = False,
        single_export: bool = False,
        single_eval: bool = False,
    ) -> Dict[str, Any]:
        """Main processing method - processes based on available data."""
        if self._has_mean_stderr_files():
            return self._process_box_plot()
        elif self._has_final_df_files():
            return self._process_multi_experiment()
        else:
            self.logger.warning("No suitable data files found for processing")
            return {}

    def _has_mean_stderr_files(self) -> bool:
        """Check if mean_stderr.csv files exist in subdirectories."""
        for root, _, files in os.walk(self.exp_path):
            if "mean_stderr.csv" in files:
                return True
        return False

    def _has_final_df_files(self) -> bool:
        """Check if final_df.csv files exist in immediate subdirectories."""
        immediate_subdirs = [
            d
            for d in self.exp_path.iterdir()
            if d.is_dir() and (d / "final_df.csv").exists()
        ]
        return len(immediate_subdirs) > 0

    def _process_box_plot(self) -> Dict[str, Any]:
        """Process data for box plot generation."""
        mean_stderr_data = self._load_mean_stderr_files()
        aggregated_data = self._transform_data(mean_stderr_data)

        # Save aggregated results
        output_path = self._save_results(aggregated_data, "final_df.csv")

        # Generate box plot
        self._generate_box_plot()

        return {
            "type": "box_plot",
            "aggregated_data": aggregated_data,
            "output_path": output_path,
        }

    def _process_multi_experiment(self) -> Dict[str, Any]:
        """Process data for multi-experiment analysis."""
        data = self._load_final_df_files()

        # Determine experiment type and generate appropriate plots
        folder_type = self._determine_multi_exp_type()

        if folder_type == "box_plot_multi":
            self._generate_box_plot_multi_exp()
        elif folder_type == "throughput_comparison":
            self.generate_multi_exp_plot()
        elif folder_type == "resource_analysis":
            self.generate_resource_plot()
            self.generate_resource_core_info()

        return {"type": "multi_experiment", "data": data}

    def _load_mean_stderr_files(self) -> List[pd.DataFrame]:
        """Load all mean_stderr.csv files from subdirectories."""
        dfs = []
        try:
            for root, _, files in os.walk(self.exp_path):
                for file in files:
                    if file == "mean_stderr.csv":
                        file_path = os.path.join(root, file)
                        data = pd.read_csv(file_path)
                        dfs.append(data)
            return dfs
        except Exception as e:
            self.logger.error(f"Error loading mean_stderr files: {e}")
            raise e

    def _load_final_df_files(self) -> Dict[str, pd.DataFrame]:
        """Load final_df.csv files from immediate subdirectories."""
        dfs = {}
        try:
            immediate_subdirs = [
                d
                for d in os.listdir(self.exp_path)
                if os.path.isdir(os.path.join(self.exp_path, d))
            ]

            for subdir in immediate_subdirs:
                subdir_path = os.path.join(self.exp_path, subdir)
                final_df_path = os.path.join(subdir_path, "final_df.csv")

                if os.path.isfile(final_df_path):
                    data = pd.read_csv(final_df_path)
                    dfs[subdir] = data
                    self.logger.info(f"Loaded {subdir} at {final_df_path}")

        except Exception as e:
            self.logger.error(f"Error loading final_df files from {self.exp_path}: {e}")
            raise

        return dfs

    def _transform_data(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Aggregate multiple dataframes into summary statistics."""
        final_df = pd.concat(dfs)
        return (
            final_df.groupby("Parallelism")
            .agg(
                Throughput_min=("Throughput", "min"),
                Throughput_max=("Throughput", "max"),
                Throughput_mean=("Throughput", "mean"),
            )
            .reset_index()
        )

    def _determine_multi_exp_type(self) -> str:
        """Determine what type of multi-experiment analysis to perform."""
        basename = self.exp_path.name.lower()

        if "resource" in basename or "flink" in basename:
            return "resource_analysis"
        elif any("tm" in str(d) for d in self.exp_path.iterdir() if d.is_dir()):
            return "box_plot_multi"
        else:
            return "throughput_comparison"

    def _generate_box_plot(self) -> None:
        """Generate box plot from mean_stderr files."""
        dfs = self._load_mean_stderr_files()
        final_df = self._transform_data(dfs)
        final_df.to_csv(os.path.join(self.exp_path, "final_df.csv"), index=False)

        boxplot_data = [
            [
                group["Throughput_min"].values[0],
                group["Throughput_mean"].values[0],
                group["Throughput_max"].values[0],
            ]
            for _, group in final_df.groupby("Parallelism")
        ]
        labels = [str(name) for name, _ in final_df.groupby("Parallelism")]

        # Get workload objective
        try:
            workload_objective = sum(
                int(gen["num_sensors"])
                for gen in Config(
                    self.logger, os.path.join(self.exp_path, "1", "exp_log.json")
                ).get(Key.Experiment.Generators.generators.key)
            )
        except Exception:
            workload_objective = 350000  # Default value

        ylim_val = (
            max(final_df["Throughput_max"].max(), workload_objective) // 100000 + 1
        ) * 100000

        # Use new strategy-based approach
        self.plotter.generate_plot(
            {
                "boxplot_data": boxplot_data,
                "labels": labels,
            },
            plot_type="whisker",
            ylim_val=ylim_val,
            workload_objective=workload_objective,
            filename="box_plot.png",
        )

    def _generate_box_plot_multi_exp(self) -> None:
        """Generate box plot for multi-experiment comparison."""
        final_dfs = self._load_final_df_files()

        if not final_dfs:
            self.logger.error("No final_df.csv files found for box plot")
            return

        boxplot_data = []
        labels = []
        sorted_keys = sorted(final_dfs.keys())

        for exp_name in sorted_keys:
            df = final_dfs[exp_name]

            required_cols = ["Throughput_min", "Throughput_max", "Throughput_mean"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"Missing required columns in {exp_name}, skipping")
                continue

            values = [
                df["Throughput_min"].values[0],
                df["Throughput_mean"].values[0],
                df["Throughput_max"].values[0],
            ]

            boxplot_data.append(values)

            # Parse experiment name format
            tm_match = re.match(r"(\d+)_tm_(\d+)_ts_per_tm", exp_name)
            if tm_match:
                num_tm = int(tm_match.group(1))
                num_ts_per_tm = int(tm_match.group(2))
                clean_label = f"{num_tm} TM,\n{num_ts_per_tm} TS/TM"
            else:
                clean_label = exp_name

            labels.append(clean_label)

        if not boxplot_data:
            self.logger.error("No valid data found for box plot")
            return

        # Reorder for specific experiments
        if len(boxplot_data) == 3:
            boxplot_data = [boxplot_data[1], boxplot_data[0], boxplot_data[2]]
            labels = [labels[1], labels[0], labels[2]]

        # Add experiment indices
        for i, label in enumerate(labels):
            exp_index = f"({'i' * (i + 1)})"
            labels[i] = f"{exp_index} {label}"

        # Use new strategy-based approach
        self.plotter.generate_plot(
            {
                "boxplot_data": boxplot_data,
                "labels": labels,
            },
            plot_type="whisker",
            ylim_val=400000,
            xlabel="TaskManager Configuration",
            ylabel="Throughput (records/s)",
            filename="multi_experiment_box_plot.png",
            workload_objective=350000,
        )

    def generate_multi_exp_plot(self, single_node: bool = True) -> None:
        """Generate multi-experiment throughput comparison plots."""
        # Machine type styling
        machine_styles = {
            "BM": {"marker": "o", "color": "#1f77b4"},
            "VM-L": {"marker": "s", "color": "#d62728"},
            "VM-S": {"marker": "D", "color": "#2ca02c"},
            "Pico": {"marker": "^", "color": "#9467bd"},
        }

        experiment_data = self._get_multi_exp_data()
        plot_data = {}
        custom_markers = {}
        custom_colors = {}
        custom_legends = {}
        custom_point_colors = {}

        # Process each experiment
        for exp_name, (config, final_df) in experiment_data.items():
            new_df = final_df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)

            is_mixed_node = "mix_node" in exp_name.lower()

            # Workaround for VM-S mixed node experiment
            if is_mixed_node and "vms" in exp_name.lower():
                self.logger.warning(
                    f"Applying workaround for VM-S mixed node experiment: {exp_name}"
                )
                if len(new_df) > 1:
                    new_df = new_df.drop(index=1).reset_index(drop=True)

            # Format index
            if single_node:
                new_df.set_index("Parallelism", inplace=True)
            else:
                new_df.reset_index(drop=True, inplace=True)
                new_df.index = new_df.index + 1

            # Export plot data
            new_df.to_csv(
                os.path.join(self.exp_path, f"{exp_name}_plot_data.csv"), index=True
            )

            # Parse machine types
            machine_types = []
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

            plot_data[display_name] = new_df["Throughput"]

            # Set visualization properties
            if is_mixed_node and len(machine_types) == 2:
                point_markers = {}
                point_colors = {}

                for idx in new_df.index:
                    marker_type = machine_types[0] if idx == 1 else machine_types[1]
                    point_markers[idx] = machine_styles.get(
                        marker_type, {"marker": "o"}
                    )["marker"]
                    point_colors[idx] = machine_styles.get(
                        marker_type, {"color": "#000000"}
                    )["color"]

                custom_markers[display_name] = point_markers
                custom_point_colors[display_name] = point_colors
                custom_colors[display_name] = machine_styles.get(
                    machine_types[0], {"color": "#000000"}
                )["color"]

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
            else:
                machine_type = machine_types[0]
                custom_markers[display_name] = machine_styles.get(
                    machine_type, {"marker": "o"}
                )["marker"]
                custom_colors[display_name] = machine_styles.get(
                    machine_type, {"color": "#000000"}
                )["color"]

        # Generate plot using new strategy-based approach
        ylabels_dict = {"Throughput": "Throughput (records/s)"}

        self.plotter.generate_plot(
            {
                "ax1_data": plot_data,
                "custom_markers": custom_markers,
                "custom_colors": custom_colors,
                "custom_point_colors": custom_point_colors,
                "custom_legends": custom_legends,
            },
            plot_type="single_frame_multiple_series",
            xlabel="Number of TaskManagers" if single_node else "Number of Machines",
            ylabels_dict=ylabels_dict,
            filename="single_node_throughput.png"
            if single_node
            else "multi_node_throughput.png",
            ylim=(0, 400000),
            axhline=350000,
            zoom_region=(0, 3, 0, 40000) if single_node else None,
        )

    def _get_multi_exp_data(self) -> Dict[str, tuple]:
        """Get multi-experiment data with configs."""
        multi_exp_data = {}
        for folder in os.listdir(self.exp_path):
            folder_path = os.path.join(self.exp_path, folder)
            final_df_path = os.path.join(folder_path, "final_df.csv")
            exp_log_path = os.path.join(folder_path, "1", "exp_log.json")
            if os.path.exists(final_df_path) and os.path.exists(exp_log_path):
                config = Config(self.logger, exp_log_path)
                final_df = pd.read_csv(final_df_path)
                multi_exp_data[folder] = (config, final_df)
        return multi_exp_data

    @staticmethod
    def _get_machine_type(name: str) -> str:
        """Extract standardized machine name from a string."""
        name = name.lower()
        if "bm" in name:
            return "BM"
        elif "vml" in name or "vm-l" in name:
            return "VM-L"
        elif "vms" in name or "vm-s" in name:
            return "VM-S"
        elif "pico" in name:
            return "Pico"
        else:
            return name.replace("single_node_", "").replace("_", " ").title()

    def generate_resource_plot(self) -> None:
        """Generate resource utilization plots."""
        resource_data = self._process_resource_data()
        final_df = pd.DataFrame(
            [
                {"cpu": cpu, "mem": mem, "throughput": throughput}
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df.to_csv(os.path.join(self.exp_path, "resource_data.csv"), index=False)

        # Use new strategy-based approach
        self.plotter.generate_plot(
            {
                "x_data": final_df["cpu"],
                "y_data": final_df["mem"],
                "z_data": final_df["throughput"],
            },
            plot_type="3d",
            title="",
            xlabel="CPU (cores)",
            ylabel="Memory (GB)",
            zlabel="Throughput (Records/s)",
            filename="resource_plot_multi_run.png",
        )

    def generate_resource_core_info(self) -> None:
        """Generate resource analysis with core utilization info."""
        nodes_config = {
            "bm": {"cpu": 52, "mem": 386},
            "vml": {"cpu": 8, "mem": 32},
            "vms": {"cpu": 2, "mem": 8},
            "pico": {"cpu": 4, "mem": 4},
        }
        resource_data = self._process_resource_data()
        final_df = pd.DataFrame(
            [
                {"cpu": cpu, "mem": mem, "throughput": throughput}
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df["tpt_per_core"] = final_df["throughput"] / final_df["cpu"]
        node_name = os.path.basename(self.exp_path).split("_")[2]
        node_config = nodes_config[node_name]
        final_df["inst_full"] = final_df.apply(
            lambda row: min(
                node_config["cpu"] // row["cpu"], node_config["mem"] // row["mem"]
            ),
            axis=1,
        )
        final_df["exp_tpt_full"] = final_df["throughput"] * final_df["inst_full"]

        final_df.sort_values(by=["cpu", "mem"], inplace=True)
        final_df.to_csv(
            os.path.join(self.exp_path, "resource_core_info.csv"), index=False
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
        with open(os.path.join(self.exp_path, "resource_core_info.tex"), "w") as file:
            file.write(latex_data)

    def _process_resource_data(self) -> Dict[tuple, float]:
        """Process resource experiment data."""
        resource_data = {}
        subdirs = [d for d in os.listdir(self.exp_path) if "flink" in d]
        for subdir in subdirs:
            subdir_path = os.path.join(self.exp_path, subdir)
            final_df_path = os.path.join(subdir_path, "final_df.csv")
            try:
                df = pd.read_csv(final_df_path)
                throughput = df["Throughput_mean"].values[0]
                match = re.search(r"flink-(\d+)m-(\d+)(-(.*))?", subdir)
                cpu, mem = int(match.group(1)) // 1000, int(match.group(2)) // 1024
                resource_data[(cpu, mem)] = throughput
            except FileNotFoundError:
                self.logger.error(
                    f"final_df.csv not found in {subdir_path}. Skipping directory."
                )
        return resource_data
