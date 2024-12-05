from datetime import datetime
from time import sleep

from scripts.monitor.experiments.Experiment import Experiment
from scripts.monitor.experiments.Scaling import Scaling
from scripts.src.data.DataEval import GroupedDataEval, DataEval
from scripts.src.data.DataExporter import DataExporter
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Tools import StoppableThread, FolderManager


class SimpleExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log

        self.runs = self.config.get_int(Key.Experiment.runs)
        self.steps = self.config.get(Key.Experiment.Scaling.steps)
        self.s: Scaling = Scaling(log, config)
        self.__log.info(
            f"[SIMPLE_E] SimpleExperiment initialized with runs: {self.runs}, steps: {self.steps}"
        )

        # Hold timestamps of all runs as a list of tuples (start, end)
        self.timestamps = []
        self.current_experiment_thread = None

    def start(self):
        self.__log.info("[SIMPLE_E] Starting experiment.")
        # Check if chaos is enabled
        if self.is_chaos_enabled():
            self.__log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )

        self.__log.info("[SIMPLE_E] Experiment started.")

    def stop(self):
        self.__log.info("[SIMPLE_E] Finishing experiment.")
        if self.current_experiment_thread:
            self.current_experiment_thread.stop()

            exp_paths = []

            f: FolderManager = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)

            # Create date folder
            date_path = f.create_date_folder(self.timestamps[0][0])

            # Create experiment folder for results
            if len(self.timestamps) > 0:
                if len(self.timestamps) > 1:
                    multi_run_folder_path = f.create_multi_run_folder()
                else:
                    multi_run_folder_path = date_path

                for i, (start_ts, end_ts) in enumerate(self.timestamps):
                    exp_path = f.create_subfolder(multi_run_folder_path)
                    exp_paths.append(exp_path)
                    log_file = self.create_log_file(exp_path, start_ts, end_ts)
                    with open(log_file, "r+") as file:
                        content = file.read()
                        file.seek(0, 0)
                        file.write(f"Experiment run {i + 1}\n\n" + content)
                    data_exp: DataExporter = DataExporter(
                        log=self.__log, exp_path=exp_path
                    )
                    data_exp.export()
                    data_eval: DataEval = DataEval(log=self.__log, exp_path=exp_path)
                    data_eval.eval_mean_stderr()

                # Get time diff since first start_ts and now
                time_diff = int(datetime.now().timestamp()) - self.timestamps[0][0]
                labels = "app=experiment-monitor"
                # Save experiment-monitor logs since time_diff seconds ago
                monitor_logs = self.s.k.pod_manager.get_logs_since(
                    labels, time_diff, "experiment-monitor"
                )
                with open(f"{multi_run_folder_path}/monitor_logs.txt", "w") as file:
                    file.write(monitor_logs)

                if len(self.timestamps) > 1:
                    data_eval_g: GroupedDataEval = GroupedDataEval(
                        log=self.__log, multi_run_path=multi_run_folder_path
                    )
                    data_eval_g.generate_box_for_means(multi_run_folder_path)
            else:
                self.__log.error("No timestamps found.")

            self.__log.info("[SIMPLE_E] Experiment finished.")

    def cleanup(self):
        self.__log.info("[SIMPLE_E] Cleaning up experiment.")
        try:
            # Remove SCHEDULABLE label from all nodes
            self.k.node_manager.reset_scaling_labels()
            self.k.node_manager.reset_state_labels()

            self.f.reset_taskmanagers()

            jobmanager_labels = "app=flink,component=jobmanager"
            self.k.pod_manager.delete_pods_by_label(jobmanager_labels, "flink")

            # delete load generators
            self.delete_load_generators()

            self.__log.info("[SIMPLE_E] Experiment cleaned up.")
        except Exception as e:
            self.__log.error(f"[SIMPLE_E] Error cleaning up: {e}")

    def running(self):
        self.__log.info("[SIMPLE_E] Running experiment.")
        self.current_experiment_thread = StoppableThread(target=self._run_experiment)
        self.s.set_stopped_callback(self.current_experiment_thread.stopped)
        self.current_experiment_thread.start()
        # Wait for the thread to finish
        self.current_experiment_thread.join()
        self.__log.info("[SIMPLE_E] Experiment run finished.")

    def _run_experiment(self):
        for run in range(self.runs):
            self.__log.info(f"[SIMPLE_E] Starting run {run + 1}")
            try:

                start_ts = int(datetime.now().timestamp())
                ret = self._single_run()
                if ret == 1:
                    self.__log.info(f"[SIMPLE_E] Exiting run {run + 1}")
                    return 1
                end_ts = int(datetime.now().timestamp())

                # Save timestamps
                self.timestamps.append((start_ts, end_ts))
                # Cleanup after each run
                self.cleanup()

                self.__log.info("[SIMPLE_E] Sleeping for 15 seconds before next run.")
                sleep(15)
                self.__log.info(
                    f"[SIMPLE_E] Run {run + 1} completed. Start: {start_ts}, End: {end_ts}"
                )

            except Exception as e:
                self.__log.error(f"[SIMPLE_E] Error during run: {e}")
                break

    def _single_run(self):
        try:
            # Deploy load generators
            self.run_load_generators()
            # Deploy job
            self.f.run_job()
            # Run scaling steps on job
            ret = self.s.run()
            if ret == 1:
                return 1
        except Exception as e:
            self.__log.error(f"[SIMPLE_E] Error during single run: {e}")
            self.cleanup()
