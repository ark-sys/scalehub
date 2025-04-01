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
        super().__init__(*args, **kwargs)
        self.__stop_event = threading.Event()
        self.__log = log
        if callable(target):
            self.target = target
        else:
            raise ValueError("Target must be a callable.")

    def stop_thread(self):
        self.__log.info("[THRD] Stopping thread.")
        self.__stop_event.set()
        self.join()

    def stopped(self):
        return self.__stop_event.is_set()

    def run(self):
        self.__log.info("[THRD] Starting thread.")
        self.target()

    def sleep(self, sleep_time):
        for _ in range(sleep_time):
            if self.stopped():
                return 1
            sleep(1)
        return 0


class Experiment:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k = KubernetesManager(log)
        self.t = Tools(log)
        self.p = Playbooks(log)
        self.current_experiment_thread = None
        self.runs = self.config.get_int(Key.Experiment.runs.key)
        self.timestamps = []

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
        if not self.timestamps:
            self.__log.warning(
                "[EXPERIMENT] No timestamps found. Skipping results creation."
            )
            return

        f = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)
        try:
            date_path = f.create_date_folder()
            multi_run_folder_path = (
                f.create_multi_run_folder() if len(self.timestamps) > 1 else date_path
            )
            for i, (start_ts, end_ts) in enumerate(self.timestamps):
                exp_path = f.create_subfolder(multi_run_folder_path)
                self.t.create_log_file(
                    self.config.to_json(), exp_path, start_ts, end_ts
                )

            time_diff = int(datetime.now().timestamp()) - self.timestamps[0][0]
            monitor_logs = self.k.pod_manager.get_logs_since(
                "app=experiment-monitor", time_diff, "experiment-monitor"
            )
            with open(f"{multi_run_folder_path}/monitor_logs.txt", "w") as file:
                file.write(monitor_logs)

            dm = DataManager(self.__log, self.config)
            dm.export(multi_run_folder_path, single_export=True, single_eval=True)
        except Exception as e:
            self.__log.error(f"[EXPERIMENT] Error during finishing: {str(e)}")

    def cleaning(self):
        try:
            self.k.node_manager.reset_scaling_labels()
            self.k.node_manager.reset_state_labels()
            self.k.statefulset_manager.reset_taskmanagers()
            self.k.pod_manager.delete_pods_by_label(
                "app=flink,component=jobmanager", "flink"
            )
            self.p.role_load_generators(self.config, tag="delete")
            self.p.reload_playbook("application/kafka", config=self.config)
        except Exception as e:
            self.__log.error(f"[EXPERIMENT] Error during cleaning: {str(e)}")

    def starting(self):
        self.__log.info("[EXPERIMENT] Starting experiment.")
        self.start_thread(self.exp)

    def exp(self):
        raise NotImplementedError("Exp method not implemented.")

    def running(self):
        self.__log.info("[EXPERIMENT] Running experiment.")
        self.join_thread()

    def do_multi_run(self, **kwargs):
        for run in range(self.runs):
            self.__log.info(f"[EXPERIMENT] Starting run {run + 1}")
            try:
                start_ts = int(datetime.now().timestamp())
                if self.single_run() == 1:
                    self.__log.info(f"[EXPERIMENT] Exiting run {run + 1}")
                    return 1
                end_ts = int(datetime.now().timestamp())
                self.timestamps.append((start_ts, end_ts))
                self.__log.info(
                    f"[EXPERIMENT] Run {run + 1} completed. Start: {start_ts}, End: {end_ts}"
                )
                if self.current_experiment_thread.sleep(10) == 1:
                    self.__log.info(f"[EXPERIMENT] Exiting run {run + 1}")
                    return 1
            except Exception as e:
                self.__log.error(f"[EXPERIMENT] Error during run: {str(e)}")
                raise e

    def single_run(self):
        try:
            s = Scaling(self.__log, self.config, self.k)
            s.set_sleep_command(self.current_experiment_thread.sleep)
            self.p.role_load_generators(self.config, tag="create")
            if s.run() == 1:
                return 1
        except Exception as e:
            self.__log.error(f"[EXPERIMENT] Error during single run: {str(e)}")
            return 1
        self.cleaning()
        return 0
