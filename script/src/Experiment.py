import os
import re
from datetime import datetime

import pandas as pd
import pytz
from matplotlib import pyplot as plt

from .Playbooks import Playbooks
from .utils.Config import Key as Key, Config
from .utils.Logger import Logger
from .utils.Misc import Misc


# Setup for an experiment:
# 1. Get timestamp at the beginning of the experiment.
# 2. Create base folder of the experiment based on the date of the day. Then, within this folder, create a folder for the Nth experiment.
# 3. Save timestamp file in the experiment folder.
# 4. Run job and Run transscale, wait for when transscale finishes <- maybe setup a trigger for this action?
# 5. Retrieve timestamp at the end of execution and write it to file.
# 6. Query VictoriaMetrics for csv data and save all files to experiment folder.
class ExperimentData:
    TPT_TIMESERIES = "flink_job_operator_throughput"
    PAR_TIMESERIES = "flink_job_operator_parallelism"

    def __init__(
        self, log: Logger, exp_path: str, start_ts: int, end_ts: int, db_url: str
    ):
        self.__log = log
        self.exp_path = exp_path
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.db_url = db_url

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
        # Export timeseries from database to csv file
        tpt_timeseries_path = self.export_data_to_csv(
            exp_path=self.exp_path,
            time_series_name=self.TPT_TIMESERIES,
            start_timestamp=self.start_ts,
            end_timestamp=self.end_ts,
        )
        par_timeseries_path = self.export_data_to_csv(
            exp_path=self.exp_path,
            time_series_name=self.PAR_TIMESERIES,
            start_timestamp=self.start_ts,
            end_timestamp=self.end_ts,
        )

        if tpt_timeseries_path is None or par_timeseries_path is None:
            self.__log.error(
                "Failed to process path for timeseries: could not join data. Something went wrong during export."
            )
            exit(1)
        else:
            df1 = pd.read_csv(tpt_timeseries_path).dropna()
            df2 = pd.read_csv(par_timeseries_path).dropna()
            if df1.empty or df2.empty:
                self.__log.error(
                    "One or both exported CSVs appear to be empty. Failed to join."
                )
                exit(1)
            else:
                # Join columns in one table and drop labels
                df_merged = df2.merge(df1).drop(
                    ["flink_job_operator_parallelism", "flink_job_operator_throughput"],
                    axis=1,
                )
                # Provide column naming
                df_merged.columns = ["Time", "Parallelism", "Throughput"]
                output_file = os.path.join(self.exp_path, "joined_output.csv")
                df_merged.to_csv(output_file, index=False)

    def eval_stats(self, skip_duration):
        filename = os.path.join(self.exp_path, "joined_output.csv")
        try:
            # Read the CSV file with comma delimiter
            df = pd.read_csv(filename, delimiter=",")

            # Convert 'Time' column to datetime
            df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)

            # Add UTC+2 to datetime
            df["Time"] = df["Time"].dt.tz_convert(pytz.timezone("Europe/Paris"))

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
            stats = df.groupby("Parallelism")["Throughput"].agg(["mean", "std"])

            # Save stats to a CSV file in the same path as the input filename
            output_path = os.path.join(os.path.dirname(filename), "stats.csv")
            stats.to_csv(output_path, index=True)
            self.__log.info(f"Stats saved to: {output_path}")
            return stats, output_path
        except FileNotFoundError:
            raise Exception(f"File not found: {filename}")
        except pd.errors.EmptyDataError:
            raise Exception(f"No data found to plot in file: {filename}")

    def eval_plot(self, stats):
        fig, ax = plt.subplots()  # Create a new figure and axes for each plot

        # Place points and error bars
        ax.errorbar(
            stats.index,
            stats["mean"],
            yerr=stats["std"],
            fmt="o",
            linestyle="-",
            capsize=4,
        )

        # Set title and axis labels
        _, date, n = self.exp_path.split("/")[-3:]
        plot_title = "Date: {} N: {}".format(date, n)
        ax.set_title(plot_title)
        ax.set(
            xlabel="Parallelism",
            ylabel="Throughput",
            xticks=list(range(1, len(stats.index) + 1)),
        )

        # Parse log file for experiment info
        log_path = os.path.join(self.exp_path, "exp_log.txt")
        job_name, num_sensors_sum, avg_interval_ms, start_ts, end_ts = Misc.parse_log(
            log_path
        )

        # Add text box with experiment info
        textstr = "\n".join(
            (
                r"job_name=%s" % (job_name,),
                r"load (sensors)=%d" % (num_sensors_sum,),
                r"rate (ms)=%d" % (avg_interval_ms,),
                r"duration (s)=%d" % (end_ts - start_ts,),
            )
        )

        props = dict(boxstyle="round", facecolor="wheat", alpha=0.5)
        ax.text(
            0.05,
            0.95,
            textstr,
            transform=ax.transAxes,
            fontsize=9,
            verticalalignment="top",
            bbox=props,
        )

        # Connect points with a line
        ax.plot(stats.index, stats["mean"], linestyle="-", marker="o", color="r")

        # Export plot to experiment path
        output_filename = os.path.join(self.exp_path, "plot.png")
        fig.savefig(output_filename)
        self.__log.info(f"Plot saved to: {output_filename}")

    # def parse_log(self, log_path: str):
    #     with open(log_path, "r") as log_file:
    #         logs = log_file.read()
    #
    #     job_name_match = re.search(r"Job name : (.+)", logs)
    #     lg_matches = re.finditer(
    #         r"LG : (.+?)\s+topic : (.+?)\s+num_sensors : (\d+)\s+interval_ms : (\d+)",
    #         logs,
    #         re.DOTALL,
    #     )
    #     start_match = re.search(r"Experiment start at : (\d+)", logs)
    #     end_match = re.search(r"Experiment end at : (\d+)", logs)
    #     if job_name_match:
    #         job_name = job_name_match.group(1)
    #     else:
    #         self.__log.error("Job name not found in log.")
    #         exit(1)
    #     if start_match and end_match:
    #         start_timestamp = int(start_match.group(1))
    #         end_timestamp = int(end_match.group(1))
    #     else:
    #         self.__log.error("Log file is incomplete: missing timestamp.")
    #         exit(1)
    #
    #     num_sensors_sum = 0
    #     interval_ms_sum = 0
    #     lg_count = 0
    #
    #     for lg_match in lg_matches:
    #         num_sensors = int(lg_match.group(3))
    #         interval_ms = int(lg_match.group(4))
    #         num_sensors_sum += num_sensors
    #         interval_ms_sum += interval_ms
    #         lg_count += 1
    #
    #     if lg_count == 0:
    #         self.__log.error("No LG information found in log.")
    #         exit(1)
    #
    #     avg_interval_ms = interval_ms_sum / lg_count
    #
    #     return (
    #         job_name,
    #         num_sensors_sum,
    #         avg_interval_ms,
    #         start_timestamp,
    #         end_timestamp,
    #     )


# class ExperimentParams:
#     experiment = ""
#     job_name = ""
#     task_name = ""
#
#     db_url = ""
#     experiment_base_path = ""
#     load_generators = []
#
#     skip_s = 0
#     plot = 0
#     stats = 0
#
#     interval = 0
#     warmup = 0
#     max_par = 0
#
#     def __init__(self, log: Logger):
#         # Initialize experiment variables
#         self.__log = log
#         self.start_ts = 0
#         self.end_ts = 0
#         self.exp_path = ""
#         self.log_file = ""
#
#     @classmethod
#     def from_config(cls, config: Config, log: Logger):
#         # Get settings from configuration file
#         cls.job_name = config.get_str(Key.JOB)
#         cls.experiment = config.get_str(Key.NAME)
#         cls.task_name = config.get_str(Key.TASK)
#
#         cls.db_url = config.get_str(Key.DB_URL)
#         cls.experiment_base_path = config.get_str(Key.EXPERIMENTS_DATA_PATH)
#         cls.load_generators = config.parse_load_generators()
#
#         cls.skip_s = config.get_int(Key.DATA_SKIP_DURATION)
#         cls.plot = config.get_bool(Key.DATA_OUTPUT_PLOT)
#         cls.stats = config.get_bool(Key.DATA_OUTPUT_STATS)
#
#         cls.interval = config.get_int(Key.TRANSSCALE_INTERVAL)
#         cls.warmup = config.get_int(Key.TRANSSCALE_WARMUP)
#         cls.max_par = config.get_int(Key.TRANSCCALE_PAR)
#         return cls
#
#     @classmethod
#     def from_logs(cls, log_path):
#         (
#             job_name,
#             num_sensors,
#             interval_ms,
#             start_ts,
#             end_ts,
#         ) = ExperimentParams.parse_log(log_path)
#         cls.job_name = job_name
#         pass
#
#     def dump_params(self):
#         log_file_path = os.path.join(self.exp_path, "exp_log.txt")
#
#         with open(log_file_path, "a") as file:
#             file.write("EXPERIMENT SPEC")
#             file.write(f"Job name : {self.job_name}\n")
#             file.write("Load generators : \n")
#             for lg_config in self.load_generators:
#                 file.write(f"   LG : {lg_config['name']}\n")
#                 file.write(f"       topic : {lg_config['topic']}\n")
#                 file.write(f"       num_sensors : {lg_config['num_sensors']}\n")
#                 file.write(f"       interval_ms : {lg_config['interval_ms']}\n")
#                 file.write(f"       replicas : {lg_config['replicas']}\n")
#                 file.write(f"       value : {lg_config['value']}\n")
#             file.write("Trasscale : \n")
#             file.write(f"    max parallelism : {self.max_par}\n")
#             file.write(f"    interval : {self.interval}\n")
#             file.write(f"    warmup : {self.warmup}\n")
#             file.write(f"\nExperiment start at : {self.start_ts}\n")
#
#     def parse_log(self, log_path: str):
#         with open(log_path, "r") as log_file:
#             logs = log_file.read()
#
#         job_name_match = re.search(r"Job name : (.+)", logs)
#         lg_matches = re.finditer(
#             r"LG : (.+?)\s+topic : (.+?)\s+num_sensors : (\d+)\s+interval_ms : (\d+)",
#             logs,
#             re.DOTALL,
#         )
#         start_match = re.search(r"Experiment start at : (\d+)", logs)
#         end_match = re.search(r"Experiment end at : (\d+)", logs)
#         if job_name_match:
#             job_name = job_name_match.group(1)
#         else:
#             self.__log.error("Job name not found in log.")
#             exit(1)
#         if start_match and end_match:
#             start_timestamp = int(start_match.group(1))
#             end_timestamp = int(end_match.group(1))
#         else:
#             self.__log.error("Log file is incomplete: missing timestamp.")
#             exit(1)
#
#         num_sensors_sum = 0
#         interval_ms_sum = 0
#         lg_count = 0
#
#         for lg_match in lg_matches:
#             num_sensors = int(lg_match.group(3))
#             interval_ms = int(lg_match.group(4))
#             num_sensors_sum += num_sensors
#             interval_ms_sum += interval_ms
#             lg_count += 1
#
#         if lg_count == 0:
#             self.__log.error("No LG information found in log.")
#             exit(1)
#
#         avg_interval_ms = interval_ms_sum / lg_count
#
#         return (
#             job_name,
#             num_sensors_sum,
#             avg_interval_ms,
#             start_timestamp,
#             end_timestamp,
#         )


class Experiment:
    def __init__(self, config: Config, log: Logger):

        self.config = config
        self.__log = log
        self.m: Misc = Misc(log)

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

    def create_log_file(self, exp_path, config):
        # Create log file
        log_file_path = os.path.join(exp_path, "exp_log.txt")

        # Dump experiment information to log file
        with open(log_file_path, "w") as file:
            file.write(f"Site : {self.site}\n")
            file.write(f"Cluster : {self.cluster}\n")
            file.write(f"Job name : {self.job_name}\n")
            file.write("Load generators : \n")
            for lg_config in config.parse_load_generators():
                file.write(f"   LG : {lg_config['name']}\n")
                file.write(f"       topic : {lg_config['topic']}\n")
                file.write(f"       num_sensors : {lg_config['num_sensors']}\n")
                file.write(f"       interval_ms : {lg_config['interval_ms']}\n")
                file.write(f"       replicas : {lg_config['replicas']}\n")
                file.write(f"       value : {lg_config['value']}\n")
            file.write("Trasscale : \n")
            file.write(f"    max parallelism : {config.get_int(Key.TRANSCCALE_PAR)}\n")
            file.write(f"    interval : {config.get_int(Key.TRANSSCALE_INTERVAL)}\n")
            file.write(f"    warmup : {config.get_int(Key.TRANSSCALE_WARMUP)}\n")
            file.write(f"\nExperiment start at : {self.start_ts}\n")
        return log_file_path

    def start_experiment(self):
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        # Create experiment folder for results
        self.exp_path = self.create_exp_folder(
            datetime.fromtimestamp(self.start_ts).strftime("%d-%m-%Y")
        )
        self.create_log_file(self)

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
        self.m.run_command(
            pod_name="flink-jobmanager",
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
        p.deploy(
            "transscale",
            job_file=self.job_name,
            task_name=self.task_name,
            max_parallelism=self.max_par,
            warmup=self.warmup,
            interval=self.interval,
        )
        result_data = self.end_experiment()
        result_data.export_experiment_data()
        if self.stats:
            stats, _ = result_data.eval_stats(self.skip_s)
            if self.plot:
                result_data.eval_plot(stats)

    # TODO clean flink jobs
    # TODO Scale down taskmanagers
    # TODO Clean transscale job

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
        result_data = self.end_experiment()
        result_data.export_experiment_data()
        if self.stats:
            stats = result_data.eval_stats(self.skip_s)
            if self.plot:
                result_data.eval_plot(stats)

    # TODO clean flink jobs
    # TODO Scale down taskmanagers
    # TODO Clean transscale job
