import os
import threading
from datetime import datetime
from time import sleep

from scripts.monitor.experiments.Scaling import Scaling
from scripts.src.data.DataManager import DataManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools, Playbooks, FolderManager


class StoppableThread(threading.Thread):
    def __init__(self, log, target=None, *args, **kwargs):
        self.__stop_event = threading.Event()
        self.__log = log
        self.target = target
        super(StoppableThread, self).__init__(*args, **kwargs)

    def stop_thread(self):
        self.__log.info("[THRD] Stopping thread.")
        self.__stop_event.set()
        self.join()

    def stopped(self):
        return self.__stop_event.is_set()

    def run(self):
        self.__log.info("[THRD] Starting thread.")
        self.target()

    def join(self, timeout=None):
        super(StoppableThread, self).join(timeout)

    def sleep(self, sleep_time):
        for i in range(sleep_time):
            if self.stopped():
                return 1
            sleep(1)
        return 0


class Experiment:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"
    TEMPLATES_BASE_PATH = "/app/templates"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k: KubernetesManager = KubernetesManager(log)
        self.t: Tools = Tools(log)
        self.p: Playbooks = Playbooks(log)

        self.current_experiment_thread = None
        self.runs = self.config.get_int(Key.Experiment.runs)

        # Experiment data
        self.timestamps = []

        # Chaos resources templates
        self.consul_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/consul-latency.yaml.j2"
        )
        self.flink_chaos_template = f"{self.TEMPLATES_BASE_PATH}/flink-latency.yaml.j2"
        self.storage_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/storage-latency.yaml.j2"
        )
        self.load_generator_deployment_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-deployment.yaml.j2"
        )
        self.load_generator_service_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-service.yaml.j2"
        )

    def start_thread(self, target):
        self.current_experiment_thread = StoppableThread(log=self.__log, target=target)
        self.current_experiment_thread.start()

    def stop_thread(self):
        if self.current_experiment_thread:
            self.current_experiment_thread.stop_thread()

    def join_thread(self):
        if self.current_experiment_thread:
            self.current_experiment_thread.join()

    def finishing(self):
        exp_paths = []

        # Create experiment folder for results
        if len(self.timestamps) > 0:
            f: FolderManager = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)
            try:
                # Create date folder
                date_path = f.create_date_folder(self.timestamps[0][0])
            except Exception as e:
                self.__log.error(f"[EXPERIMENT] Error creating date folder: {str(e)}")
                return None

            try:
                if len(self.timestamps) > 1:
                    multi_run_folder_path = f.create_multi_run_folder()
                else:
                    multi_run_folder_path = date_path

                for i, (start_ts, end_ts) in enumerate(self.timestamps):
                    exp_path = f.create_subfolder(multi_run_folder_path)
                    exp_paths.append(exp_path)
                    # Create log file
                    log_file_path = os.path.join(exp_path, "exp_log.txt")
                    try:
                        # Dump experiment information to log file
                        with open(log_file_path, "w") as file:
                            file.write(f"Experiment run {i + 1}\n\n")
                            file.write(f"[CONFIG]\n")
                            file.write(f"{self.config.to_json()}\n\n")
                            file.write(f"[TIMESTAMPS]\n")
                            file.write(f"Experiment start at : {start_ts}\n")
                            file.write(f"Experiment end at : {end_ts}\n")
                        return log_file_path
                    except Exception as e:
                        self.__log.error(f"[EXP] Error creating log file: {str(e)}")
                try:
                    # Get time diff since first start_ts and now
                    time_diff = int(datetime.now().timestamp()) - self.timestamps[0][0]
                    labels = "app=experiment-monitor"
                    # Save experiment-monitor logs since time_diff seconds ago
                    monitor_logs = self.k.pod_manager.get_logs_since(
                        labels, time_diff, "experiment-monitor"
                    )
                    with open(f"{multi_run_folder_path}/monitor_logs.txt", "w") as file:
                        file.write(monitor_logs)
                    try:
                        # Export data
                        dm: DataManager = DataManager(self.__log, self.config)
                        dm.export(multi_run_folder_path)
                    except Exception as e:
                        self.__log.error(f"[EXPERIMENT] Error exporting data: {str(e)}")
                        return None
                except Exception as e:
                    self.__log.error(
                        f"[EXPERIMENT] Error saving monitor logs: {str(e)}"
                    )
                    return None
            except Exception as e:
                self.__log.error(
                    f"[EXPERIMENT] Error creating experiment folder: {str(e)}"
                )
                return None
        else:
            self.__log.warning(
                "[EXPERIMENT] No timestamps found. Skipping results creation."
            )

    def cleaning(self):
        try:
            # Remove SCHEDULABLE label from all nodes
            self.k.node_manager.reset_scaling_labels()
            self.k.node_manager.reset_state_labels()
        except Exception as e:
            self.__log.error(
                f"[EXPERIMENT] Error cleaning up - resetting labels : {str(e)}"
            )
        try:
            # Reset taskmanagers to 0
            self.k.statefulset_manager.reset_taskmanagers()

            # Reset jobmanager
            jobmanager_labels = "app=flink,component=jobmanager"
            self.k.pod_manager.delete_pods_by_label(jobmanager_labels, "flink")
        except Exception as e:
            self.__log.error(
                f"[EXPERIMENT] Error cleaning up - resetting flink: {str(e)}"
            )
        try:
            # Delete load generators
            self.p.role_load_generators(self.config, tag="delete")
        except Exception as e:
            self.__log.error(
                f"[EXPERIMENT] Error cleaning up - deleting load generators: {str(e)}"
            )
        try:
            # Reload kafka
            self.p.reload_playbook("application/kafka", config=self.config)
        except Exception as e:
            self.__log.error(
                f"[EXPERIMENT] Error cleaning up - reloading kafka: {str(e)}"
            )

    def starting(self):
        self.__log.info("[EXPERIMENT] Starting experiment.")

        # Create a new thread for the experiment
        self.start_thread(self.exp)

    def exp(self):
        raise NotImplementedError("Exp method not implemented.")

    def running(self):
        self.__log.info("[EXPERIMENT] Running experiment.")
        self.join_thread()

    def do_multi_run(self):
        # Do a single_run self.runs times and save timestamps for each run
        for run in range(self.runs):
            self.__log.info(
                f"[EXPERIMENT] =================================== Starting run {run + 1} ==================================="
            )
            try:
                # Get start timestamp of this run
                start_ts = int(datetime.now().timestamp())
                # Execute single run
                ret = self.__single_run()

                if ret == 1:
                    # Run was stopped
                    self.__log.info(f"[EXPERIMENT] Exiting run {run + 1}")
                    return 1

                # Get end timestamp of this run
                end_ts = int(datetime.now().timestamp())

                # Save timestamps
                self.timestamps.append((start_ts, end_ts))
                self.__log.info(
                    f"[EXPERIMENT] Run {run + 1} completed. Start: {start_ts}, End: {end_ts}"
                )
                self.__log.info("[EXPERIMENT] Sleeping for 10 seconds before next run.")
                ret = self.current_experiment_thread.sleep(10)
                if ret == 1:
                    self.__log.info(f"[EXPERIMENT] Exiting run {run + 1}")
                    return 1

            except Exception as e:
                self.__log.error(f"[EXPERIMENT] Error during run: {str(e)}")
                return 1

    def __single_run(self):
        try:
            # Get current config
            config = self.config

            # Create scaling object
            s = Scaling(self.__log, config, self.k)
            s.set_sleep_command(self.current_experiment_thread.sleep)

            # Create load generators
            self.p.role_load_generators(self.config, tag="create")

            # Run scaling steps on job
            ret = s.run()
            if ret == 1:
                return 1
            # Cleaup after each run
            self.cleaning()
        except Exception as e:
            self.__log.error(f"[EXPERIMENT] Error during single run: {str(e)}")
            return 1
