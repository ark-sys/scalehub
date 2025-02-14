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

    @staticmethod
    def __aggregate_data(dfs):
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
            comment=Config(
                self.__log, os.path.join(self.base_path, "1", "exp_log.json")
            ).get(Key.Experiment.comment.key),
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
        experiment_data = self._get_multi_exp_data()
        plot_data = {}
        for exp_name, (config, final_df) in experiment_data.items():
            new_df = final_df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)
            new_df.set_index("Parallelism", inplace=True)
            new_df.to_csv(
                os.path.join(self.base_path, f"{exp_name}_plot_data.csv"), index=True
            )
            exp_name = exp_name.replace("single_node_", "").replace("_", " ").title()
            plot_data[exp_name] = new_df["Throughput"]

        ylabels_dict = {"Throughput": "Throughput (records/s)"}
        if single_node:
            self.plotter.generate_single_frame_multiple_series_plot(
                ax1_data=plot_data,
                xlabel="Number of TaskManagers",
                ylabels_dict=ylabels_dict,
                filename=os.path.join(self.base_path, "single_node_throughput.png"),
                ylim=(0, 400000),
                axhline=350000,
                zoom_region=(0, 4, 0, 60000),
            )
        else:
            self.plotter.generate_single_frame_multiple_series_plot(
                ax1_data=plot_data,
                xlabel="Number of Machines",
                ylabels_dict=ylabels_dict,
                filename=os.path.join(self.base_path, "multi_node_throughput.png"),
                ylim=(0, 400000),
                axhline=350000,
            )

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
