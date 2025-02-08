import os
import re

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

from scripts.src.data.Plotter import Plotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class GroupedDataEval:
    def __init__(self, log: Logger, exp_path: str):
        self.__log: Logger = log
        self.t: Tools = Tools(log)
        self.plotter = Plotter(self.__log, plots_path=exp_path)

        self.base_path = exp_path

    # Load the mean_stderr.csv file and generate a boxplot
    def generate_box_for_means(self):
        exp_path = os.path.join(self.base_path, "1", "exp_log.json")
        config: Config = Config(self.__log, exp_path)

        dfs = []
        for root, dirs, files in os.walk(self.base_path):
            for file in files:
                if file == "mean_stderr.csv":
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    dfs.append(df)

        num_runs = len(dfs)
        final_df = pd.concat(dfs)

        final_df = (
            final_df.groupby("Parallelism")
            .agg(
                Throughput_min=("Throughput", "min"),
                Throughput_max=("Throughput", "max"),
                Throughput_mean=("Throughput", "mean"),
            )
            .reset_index()
        )

        # Save new data df to file
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

        ax.set_xlabel("Number of slots", fontsize=24)
        ax.set_ylabel("Throughput (records/s)", fontsize=24)
        plt.xticks(fontsize=20)
        ax.tick_params(axis="y", labelsize=20)

        # Get Workload objective from logs
        workload_objective = 0
        for generator in config.get(Key.Experiment.Generators.generators.key):
            workload_objective += int(generator["num_sensors"])

        # Add straight dotted line at y=100000, This is the target objective for the experiment
        ax.axhline(
            y=workload_objective, color="r", linestyle="--", label="Workload objective"
        )

        # eval ylim based on max value in the data. Round up to the nearest 100000. ex. 310000 -> 400000
        ylim_val = (
            max(final_df["Throughput_max"].max(), workload_objective) // 100000 + 1
        ) * 100000

        ax.set_ylim(0, ylim_val)

        # Add a legend
        # ax.legend(loc="upper right", fontsize=22)

        # # Get comment from config
        # comment = config.get(Key.Experiment.comment.key)
        # ax.set_title(f"{comment}", fontsize=24)

        fig.tight_layout()
        output_path = os.path.join(self.base_path, f"boxplot_{num_runs}_runs_mean.png")
        fig.savefig(output_path)
        plt.close(fig)

    def is_single_node(self):
        if any("single_node" in s for s in os.listdir(self.base_path)):
            return True

    def __get_multi_exp_dict(self):
        multi_exp_data = {}
        # Get folder names in the base path
        for folder in os.listdir(self.base_path):
            folder_path = os.path.join(self.base_path, folder)

            # Get final_df.csv file
            final_df_path = os.path.join(folder_path, "final_df.csv")

            # Get config file
            exp_log_path = os.path.join(folder_path, "1", "exp_log.json")

            if os.path.exists(final_df_path) and os.path.exists(exp_log_path):
                config = Config(self.__log, exp_log_path)
                final_df = pd.read_csv(final_df_path)

                multi_exp_data[folder] = (config, final_df)

        return multi_exp_data

    def generate_multi_exp_multi_node_plot(self):
        experiment_data = self.__get_multi_exp_dict()

        plot_data = {}

        for exp_name, (config, final_df) in experiment_data.items():
            # We need to filter final_df to only include the parallelism amount defined in config
            steps = config.get(Key.Experiment.Scaling.steps.key)
            parallelism_per_node = {}
            total_taskmanagers = 0
            for step in steps:
                node_base_name = step["node"]
                node_suffix = 1
                node_name = f"{node_base_name}-{node_suffix}"
                while node_name in parallelism_per_node:
                    node_suffix += 1
                    node_name = f"{node_base_name}-{node_suffix}"

                taskmanagers = sum([tm["number"] for tm in step["taskmanager"]])
                total_taskmanagers += taskmanagers
                parallelism_per_node[node_name] = (taskmanagers, total_taskmanagers)

            expected_par_values = [value[1] for value in parallelism_per_node.values()]
            # Filter final_df to only include the parallelism values in expected_par_values
            filtered_df = final_df[final_df["Parallelism"].isin(expected_par_values)]
            #
            # # Add node name to final_df
            # final_df["Node"] = final_df["Parallelism"].apply(
            #     lambda x: list(parallelism_per_node.keys())[
            #         expected_par_values.index(x)
            #     ]
            # )

            # Reset index
            filtered_df.reset_index(drop=True, inplace=True)

            # Change Throughput_mean to Throughput
            # Create a new DataFrame with only the Throughput and Parallelism columns
            new_df = filtered_df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)
            new_df.index += 1
            plot_data[exp_name] = new_df["Throughput"]

            # Save new data df to file
            new_df.to_csv(
                os.path.join(self.base_path, f"{exp_name}_plot_data.csv"), index=True
            )

        self.plotter.generate_single_plot_multiple_series(
            plot_data,
            xlabel="Number of Machines",
            ylabels_dict={"Throughput": "Throughput (records/s)"},
            filename=os.path.join(self.base_path, "multi_node_throughput.png"),
            ylim=(0, 400000),
            axhline=350000,
        )

    def generate_multi_exp_single_node_plot(self):
        # Here we just need to get the Throughput_mean values for each experiment and Parallelism will be the index
        experiment_data = self.__get_multi_exp_dict()

        plot_data = {}

        for exp_name, (config, final_df) in experiment_data.items():
            new_df = final_df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)

            # Set Parallelism as index
            new_df.set_index("Parallelism", inplace=True)

            # Save a copy of the new DataFrame to a CSV file
            new_df.to_csv(
                os.path.join(self.base_path, f"{exp_name}_plot_data.csv"), index=True
            )

            # Refactor name: remove single_node, replace _ with space, and capitalize letters before number
            exp_name = exp_name.replace("single_node_", "")

            exp_name = (
                (exp_name.split("_")[0].upper() + " " + exp_name.split("_")[1])
                if "_" in exp_name
                else exp_name.upper()
            )

            plot_data[exp_name] = new_df["Throughput"]

        zoom_region = (0, 4, 0, 60000)

        self.plotter.generate_single_plot_multiple_series(
            plot_data,
            xlabel="Number of TaskManagers",
            ylabels_dict={"Throughput": "Throughput (records/s)"},
            filename=os.path.join(self.base_path, "single_node_throughput.png"),
            ylim=(0, 400000),
            axhline=350000,
            zoom_region=zoom_region,
        )

    def process_resource_data(self):
        resource_data = {}

        subdirs = [d for d in os.listdir(self.base_path) if "flink" in d]
        for subdir in subdirs:
            subdir_path = os.path.join(self.base_path, subdir)

            # Retrieve final_df.csv from the subdir
            final_df_path = os.path.join(subdir_path, "final_df.csv")
            try:
                df = pd.read_csv(final_df_path)
                throughput = df["Throughput_mean"].values[0]
                pod_config = r"flink-(\d+)m-(\d+)(-(.*))?"
                match = re.search(pod_config, subdir)
                cpu, mem = match.group(1), match.group(2)
                resource_data[(int(cpu) // 1000, int(mem) // 1024)] = throughput

            except FileNotFoundError:
                self.__log.error(
                    f"final_df.csv not found in {subdir_path}. Skipping directory."
                )

        return resource_data

    def generate_resource_plot(self):
        resource_data = self.process_resource_data()
        final_df = pd.DataFrame(
            [
                {
                    "cpu": cpu,
                    "mem": mem,
                    "throughput": throughput,
                }
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df = final_df.apply(pd.to_numeric, errors="coerce").dropna()

        # Save the final DataFrame to a CSV file
        final_df.to_csv(os.path.join(self.base_path, "resource_data.csv"), index=False)

        # Pass the final DataFrame to self.plotter.generate_3d_plot() for 3D plot generation
        self.plotter.generate_3d_plot(
            final_df["cpu"],
            final_df["mem"],
            final_df["throughput"],
            title="",
            xlabel="CPU (cores)",
            ylabel="Memory (GB)",
            zlabel="Throughput (Records/s)",
            filename="resource_plot_multi_run.png",
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
                {
                    "cpu": cpu,
                    "mem": mem,
                    "throughput": throughput,
                }
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        final_df = final_df.apply(pd.to_numeric, errors="coerce").dropna()

        final_df["tpt_per_core"] = final_df["throughput"] / final_df["cpu"]

        # Get node_name from self.base_path
        node_name = os.path.basename(self.base_path).split("_")[2]

        node_config = nodes_config[node_name]

        final_df["inst_full"] = final_df.apply(
            lambda row: min(
                node_config["cpu"] // row["cpu"], node_config["mem"] // row["mem"]
            ),
            axis=1,
        )

        final_df["exp_tpt_full"] = final_df["throughput"] * final_df["inst_full"]

        # Save the final DataFrame to a CSV file
        final_df.to_csv(
            os.path.join(self.base_path, "resource_core_info.csv"), index=False
        )

        best_row = final_df.loc[final_df["exp_tpt_full"].idxmax()]

        # Prepare latex data
        latex_data = final_df.o_latex(
            index=False,
            float_format="%.2f",
            escape=True,
            caption=f"Throughput evaluation for {node_name.upper()}",
        )

        # Create the best row configuration string
        best_row_string = (
            f"Best throughput per core of {round(best_row['throughput'], 2)} is achieved with {int(best_row['cpu'])} CPUs, {round(best_row['mem'], 1)} GB of memory."
            f"With this configuration, a {node_name} machine can host {int(best_row['inst_full'])} instances, with an expected throughput of {round(best_row['exp_tpt_full'], 2)}."
        )

        latex_data = latex_data.replace(
            "\\end{tabular}", "\\end{tabular}\n" f"\\textbf{{{best_row_string}}}\n"
        )

        latex_file_path = os.path.join(self.base_path, "resource_core_info.tex")
        with open(latex_file_path, "w") as file:
            file.write(latex_data)
