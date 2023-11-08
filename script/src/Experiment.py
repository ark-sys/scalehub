import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import pandas.errors
import pytz
import yaml
from matplotlib import pyplot as plt
from matplotlib.legend_handler import HandlerTuple

from .Playbooks import Playbooks
from .utils.Config import Key as Key, Config
from .utils.Defaults import DefaultValues as Values
from .utils.Logger import Logger
from .utils.Misc import Misc
from .utils.KubernetesManager import KubernetesManager

# Setup for an experiment:
# 1. Get timestamp at the beginning of the experiment.
# 2. Create base folder of the experiment based on the date of the day. Then, within this folder, create a folder for the Nth experiment.
# 3. Save timestamp file in the experiment folder.
# 4. Run job and Run transscale, wait for when transscale finishes <- maybe setup a trigger for this action?
# 5. Retrieve timestamp at the end of execution and write it to file.
# 6. Query VictoriaMetrics for csv data and save all files to experiment folder.
class ExperimentData:
    BASE_TIMESERIES = "flink_operator"

    def __init__(
        self, log: Logger, exp_path: str, start_ts: int, end_ts: int, db_url: str
    ):
        self.__log = log
        self.exp_path = exp_path
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.db_url = db_url
        # Placeholders for throughput_in, throughput_out and parallelism timeseries
        self.tpo_timeseries = ""
        self.tpi_timeseries = ""
        self.par_timeseries = ""

    def export_data_to_csv(
        self,
        exp_path: str,
        time_series_name: str,
        start_timestamp: int = None,
        end_timestamp: int = None,
        format_labels="__name__,__timestamp__:unix_s,__value__",
    ):
        if start_timestamp is None or end_timestamp is None:
            self.__log.error("Missing export timestamp for timeseries.")
            exit(1)

        # VictoriaMetrics export CSV api
        api_url = f"http://{self.db_url}/api/v1/export/csv"
        params = {
            "format": format_labels,
            "match": time_series_name,
            "start": start_timestamp,
            "end": end_timestamp,
        }
        import requests

        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            output_file = os.path.join(exp_path, f"{time_series_name}_export.csv")
            with open(output_file, "wb") as file:
                file.write(response.content)
            self.__log.info(f"Data exported to {output_file}")
            return output_file
        else:
            self.__log.error(f"Error exporting data: {response.text}")

    def export_experiment_data(self):
        # Check from logs for which operator name timeseries should be exported*
        log_path = os.path.join(self.exp_path, "exp_log.txt")
        with open(log_path, "r") as log_file:
            content = log_file.read()
        operator_name_match = re.search(r"experiment.task_name =\s*(\w+)", content)
        if operator_name_match:
            operator_name = operator_name_match.group(1)
            self.__log.info(f"Retrieving timeseries for operator {operator_name}")
            self.tpo_timeseries = (
                f"{self.BASE_TIMESERIES}_{operator_name}_throughput_out"
            )
            self.tpi_timeseries = (
                f"{self.BASE_TIMESERIES}_{operator_name}_throughput_in"
            )
            self.par_timeseries = f"{self.BASE_TIMESERIES}_{operator_name}_parallelism"
        else:
            self.__log.warning("Could not find operator name for timeseries export")
            exit(1)

        # Export timeseries from database to csv file
        tpo_timeseries_path = self.export_data_to_csv(
            exp_path=self.exp_path,
            time_series_name=self.tpo_timeseries,
            start_timestamp=self.start_ts,
            end_timestamp=self.end_ts,
        )
        par_timeseries_path = self.export_data_to_csv(
            exp_path=self.exp_path,
            time_series_name=self.par_timeseries,
            start_timestamp=self.start_ts,
            end_timestamp=self.end_ts,
        )

        tpi_timeseries_path = self.export_data_to_csv(
            exp_path=self.exp_path,
            time_series_name=self.tpi_timeseries,
            start_timestamp=self.start_ts,
            end_timestamp=self.end_ts,
        )

        if (
            tpo_timeseries_path is None
            or par_timeseries_path is None
            or tpi_timeseries_path is None
        ):
            self.__log.error(
                "Failed to process path for timeseries: could not join data. Something went wrong during export."
            )
            exit(1)
        else:
            df1 = pd.read_csv(tpo_timeseries_path).dropna()
            df2 = pd.read_csv(par_timeseries_path).dropna()
            if df1.empty or df2.empty:
                self.__log.error(
                    "One or both exported CSVs appear to be empty. Failed to join."
                )
                exit(1)
            else:
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

                output_file = os.path.join(self.exp_path, "joined_output.csv")
                df_merged.dropna().to_csv(output_file, index=False)

    # This function takes a join.csv file, which contains throughput and parallelism metric from an experiment And
    # evaluates mean and stdev throughput for each parallelism. If predictions are found for experiment,
    # they are added to output file named stats.csv
    def eval_stats(self, skip_duration):
        def extract_predictions(log_path) -> dict[Any, list[Any]]:
            predictions = {}
            target_pattern = r"((?:.*\n){14}.*Re-configuring PARALLELISM\n.*Current Parallelism: (\d+)\n.*Target Parallelism: (\d+))"

            with open(log_path, "r") as log_file:
                logs = log_file.read()

                # Extract strings from match to match-15 lines
                match_blocks = re.findall(target_pattern, logs)
                for match in match_blocks:
                    block = match[0].strip()
                    target_parallelism = match[2]
                    tpt_pattern = rf"\bpar, transp: {target_parallelism}, 1 target tput: \d+ new_tput %: (\d+\.\d+)"
                    prediction = re.search(tpt_pattern, block)
                    if prediction:
                        prediction_value = float(prediction.group(1))

                        # Initialize an empty list if target_parallelism doesn't exist in predictions
                        if target_parallelism not in predictions:
                            predictions[target_parallelism] = []

                        # Calculate region
                        region = len(predictions[target_parallelism]) + 1

                        predictions[target_parallelism].append((region, prediction_value))
                # Return list of predictions
                return predictions
        filename = os.path.join(self.exp_path, "joined_output.csv")
        try:
            # Read the CSV file with comma delimiter
            df = pd.read_csv(filename, delimiter=",")

            # Convert 'Time' column to datetime
            df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)

            # Add UTC+2 to datetime
            df["Time"] = df["Time"].dt.tz_convert(pytz.timezone("Europe/Paris"))

            # Make sure that Parallelism values are integers and not floats
            df["Parallelism"] = df["Parallelism"].astype(int)

            # Skip the specified duration for each degree of parallelism
            df = (
                df.groupby("Parallelism")
                .apply(
                    lambda x: x[
                        x["Time"]
                        >= x["Time"].min() + pd.Timedelta(seconds=skip_duration)
                    ]
                )
                .reset_index(drop=True)
            )

            # Calculate mean throughput and standard deviation for each parallelism
            stats_tpo = df.groupby("Parallelism")["Throughput_OUT"].agg(["mean", "std"])
            stats_tpi = df.groupby("Parallelism")["Throughput_IN"].agg(["mean", "std"])
            # Combine the mean and std for 'Throughput' and 'Input' into a single DataFrame
            stats = pd.concat(
                [stats_tpo, stats_tpi], axis=1, keys=["Throughput_OUT", "Throughput_IN"]
            )
            # Reset index for better formatting
            stats.reset_index(inplace=True)
            # Check if transscale log exists
            transscale_log = os.path.join(self.exp_path, "transscale_log.txt")
            if os.path.exists(transscale_log):
                # Transscale log found, try to extract predictions
                self.__log.info("Transscale log found. Preparing predictions export.")
                predictions = extract_predictions(transscale_log)

                # Create a new DataFrame from the dictionary
                pred_df = pd.DataFrame(
                    [(int(k), *i) for k, v in predictions.items() for i in v],
                    columns=["Parallelism", "region", "Predictions"],
                )
                # # Set 'Parallelism' and 'region' as index in the new DataFrame
                pred_df.set_index(["Parallelism", "region"], inplace=True)
                stats = pd.concat([stats, pred_df], axis=1)

            # Save stats to a CSV file in the same path as the input filename
            output_path = os.path.join(os.path.dirname(filename), "stats.csv")
            stats.to_csv(output_path, index=False)
            self.__log.info(f"Stats saved to: {output_path}")
            return stats, output_path
        except FileNotFoundError:
            raise Exception(f"File not found: {filename}")
        except pd.errors.EmptyDataError:
            raise Exception(f"No data found to plot in file: {filename}")

    def eval_plot(self, stats):
        fig, ax = plt.subplots()  # Create a new figure and axes for plot

        # Parse log file for experiment info
        log_path = os.path.join(self.exp_path, "exp_log.txt")
        m: Misc = Misc(self.__log)
        job_name, num_sensors_sum, avg_interval_ms, start_ts, end_ts = m.parse_log(
            log_path
        )

        ax.errorbar(
            stats["Parallelism"],
            stats["Throughput_IN"]["mean"],
            yerr=stats["Throughput_IN"]["std"],
            fmt="o",
            linestyle="-",
            color="b",
            capsize=4,
            label="Throughput In",
        )

        # Check if 'Predictions' column exists, if so, add dashed line with its values to plot
        if "Predictions" in stats.columns:
            ax.plot(
                stats["Parallelism"],
                stats["Predictions"],
                linestyle="--",
                marker="o",
                color="r",
                label="Predictions",
            )

        # Set title and axis labels
        _, date, n = self.exp_path.split("/")[-3:]
        operator_name = job_name.split("-")[0]
        plot_title = "Join" if operator_name == "myjoin" else "Map"
        ax.set_title(plot_title)
        ax.set(
            xlabel="Parallelism",
            ylabel="Throughput (records/s)",
            xticks=list(range(1, len(stats.index) + 1)),
        )

        ax.legend(loc="lower right", handler_map={tuple: HandlerTuple(ndivide=None)})

        # Export plot to experiment path
        output_filename = os.path.join(self.exp_path, "plot.png")
        fig.savefig(output_filename)
        self.__log.info(f"Plot saved to: {output_filename}")


class Experiment:
    def __init__(self, config: Config, log: Logger):

        self.config = config
        self.__log = log
        self.m: Misc = Misc(log)
        self.k: KubernetesManager = KubernetesManager(log)

        self.cluster = config.get_str(Key.CLUSTER)
        self.site = config.get_str(Key.SITE)

        # Get settings from configuration file
        self.job_name = config.get_str(Key.JOB)
        self.experiment = config.get_str(Key.NAME)
        self.task_name = config.get_str(Key.TASK)

        self.db_url = config.get_str(Key.DB_URL)
        self.experiment_base_path = config.get_str(Key.EXPERIMENTS_DATA_PATH)

        self.skip_s = config.get_int(Key.DATA_SKIP_DURATION)
        self.plot = config.get_bool(Key.DATA_OUTPUT_PLOT)
        self.stats = config.get_bool(Key.DATA_OUTPUT_STATS)

        self.interval = config.get_int(Key.TRANSSCALE_INTERVAL)
        self.warmup = config.get_int(Key.TRANSSCALE_WARMUP)
        self.max_par = config.get_int(Key.TRANSCCALE_PAR)

        # Initialize experiment variables
        self.start_ts = 0
        self.end_ts = 0
        self.exp_path = ""
        self.log_file = ""

    def create_exp_folder(self, date):
        # Create the base folder path
        base_folder_path = os.path.join(self.experiment_base_path, date)
        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1

        # Create the subfolder
        os.makedirs(subfolder_path)

        # Return the path to the new subfolder
        return subfolder_path

    def create_log_file(self, exp_path):
        # Create log file
        log_file_path = os.path.join(exp_path, "exp_log.txt")

        # Dump experiment information to log file
        with open(log_file_path, "w") as file, open(Values.System.CONF_PATH) as config:
            for line in config:
                file.write(line)
            file.write(f"\nExperiment start at : {self.start_ts}\n")
        return log_file_path

    def start_experiment(self):
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        # Create experiment folder for results
        self.exp_path = self.create_exp_folder(
            datetime.fromtimestamp(self.start_ts).strftime("%d-%m-%Y")
        )
        self.log_file = self.create_log_file(self.exp_path)

    def end_experiment(self):
        # Get finish timestamp
        self.end_ts = int(datetime.now().timestamp())

        # Dump information to log file
        with open(self.log_file, "a") as file:
            file.write(f"Experiment end at : {self.end_ts}\n")

        data: ExperimentData = ExperimentData(
            log=self.__log,
            exp_path=self.exp_path,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            db_url=self.db_url,
        )

        return data

    def full_run(self):
        p: Playbooks = Playbooks()
        # Launch Flink Job
        self.k.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink run -d -j /tmp/jobs/{self.job_name}",
        )
        # Start load generators
        for lg_config in self.config.parse_load_generators():
            p.deploy(
                "load_generators",
                lg_name=lg_config["name"],
                lg_topic=lg_config["topic"],
                lg_numsensors=int(lg_config["num_sensors"]),
                lg_intervalms=int(lg_config["interval_ms"]),
                lg_replicas=int(lg_config["replicas"]),
                lg_value=int(lg_config["value"]),
            )
        self.start_experiment()
        # transscale_result = p.deploy(
        #     "transscale",
        #     job_file=self.job_name,
        #     task_name=self.task_name,
        #     max_parallelism=self.max_par,
        #     warmup=self.warmup,
        #     interval=self.interval,
        # )

        transscale_params = {
            "job_file": self.job_name,
            "task_name": self.task_name,
            "max_parallelism": self.max_par,
            "warmup": self.warmup,
            "interval": self.interval,
        }

        transscale_playbook = (
            f"{self.config.get_str(Key.PLAYBOOKS_PATH)}/roles/transscale"
        )
        vars_file = os.path.join(transscale_playbook, "vars/main.yaml")
        job_file = os.path.join(transscale_playbook, "templates/transscale-job.yaml.j2")

        # Add image and tag definition from values
        try:
            with open(vars_file) as vars:
                transscale_params.update(yaml.safe_load(vars))
        except FileNotFoundError as e:
            print(f"Config file not found: {e}")

        p.deploy(
            "transscale",
            job_file=self.job_name,
            task_name=self.task_name,
            max_parallelism=self.max_par,
            warmup=self.warmup,
            interval=self.interval,
        )
        # Retrieve the logs from the execution of transscale
        transscale_logs = self.k.run_job(job_file, params=transscale_params)
        # transscale_logs = transscale_result.to_dict(include_payload=True)[-1][
        #     "payload"
        # ]["log_lines"]

        # Generate the output path where the logs will be saved
        transscale_log_path = os.path.join(self.exp_path, "transscale_log.txt")

        # Save logs to file
        # log_lines_string = "\n".join(transscale_logs)
        with open(transscale_log_path, "w") as file:
            file.write(transscale_logs)

        # Trigger the end of the experiment
        result_data = self.end_experiment()

        # Save exported data from VM to file
        result_data.export_experiment_data()

        if self.stats:
            stats, _ = result_data.eval_stats(self.skip_s)
            if self.plot:
                result_data.eval_plot(stats)

        # Clean flink jobs
        self.k.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="for job_id in $(flink list -r | awk -F ' : ' ' {print $2}'); do flink cancel $job_id ;done",
        )

        # Scale down taskmanagers
        self.k.scale_deployment("flink-taskmanager")
        # Clean transscale job
        self.k.delete_job("transscale-job")
        # Remove load generators
        for lg_config in self.config.parse_load_generators():
            p.delete(
                "load_generators",
                lg_name=lg_config["name"],
                lg_topic=lg_config["topic"],
                lg_numsensors=int(lg_config["num_sensors"]),
                lg_intervalms=int(lg_config["interval_ms"]),
                lg_replicas=int(lg_config["replicas"]),
                lg_value=int(lg_config["value"]),
            )

    def transscale_only_run(self):
        self.start_experiment()
        p: Playbooks = Playbooks()
        p.deploy(
            "transscale",
            job_file=self.job_name,
            task_name=self.task_name,
            max_parallelism=self.max_par,
            warmup=self.warmup,
            interval=self.interval,
        )
        # result_data = self.end_experiment()
        # result_data.export_experiment_data()
        # if self.stats:
        #     stats, _ = result_data.eval_stats(self.skip_s)
        #     if self.plot:
        #         result_data.eval_plot(stats)
        #
        # # Clean flink jobs
        # self.m.execute_command_on_pod(
        #     deployment_name="flink-jobmanager",
        #     command="for job_id in $(flink list -r | awk -F ' : ' '/\(RUNNING\)/ {print $2}'); do flink cancel $job_id ;done",
        # )
        #
        # # Scale down taskmanagers
        # self.m.scale_deployment("flink-taskmanager")
        # Clean transscale job
        self.k.delete_job("transscale-job")
