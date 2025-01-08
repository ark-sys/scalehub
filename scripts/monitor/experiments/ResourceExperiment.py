from datetime import datetime

from scripts.monitor.experiments.Experiment import Experiment
from scripts.monitor.experiments.Scaling import Scaling
from scripts.src.data.DataManager import DataManager
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Tools import FolderManager


class ResourceExperiment(Experiment):
    memory_values = [1024, 2048, 4096, 8192, 16384, 32768]
    cores_values = ["1000m", "2000m", "4000m", "8000m", "16000m", "32000m"]

    def __init__(self, log, config):
        super().__init__(log, config)
        self.timestamps = []
        self.__log = log

        self.runs = self.config.get_int(Key.Experiment.runs)

    def starting(self):
        self.__log.info("[RESOURCE_E] Starting experiment.")
        self.start_thread(self.__run_experiment)

    def finishing(self):
        self.__log.info("[RESOURCE_E] Finishing experiment.")

        exp_paths = []

        # Create experiment folder for results
        if len(self.timestamps) > 0:
            f: FolderManager = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)
            try:
                # Create date folder
                date_path = f.create_date_folder(self.timestamps[0][0])
            except Exception as e:
                self.__log.error(f"[RESOURCE_E] Error creating date folder: {str(e)}")
                return None

            try:
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
                        self.__log.error(f"[RESOURCE_E] Error exporting data: {str(e)}")
                        return None
                except Exception as e:
                    self.__log.error(
                        f"[RESOURCE_E] Error saving monitor logs: {str(e)}"
                    )
                    return None
            except Exception as e:
                self.__log.error(
                    f"[RESOURCE_E] Error creating experiment folder: {str(e)}"
                )
                return None
        else:
            self.__log.warning(
                "[RESOURCE_E] No timestamps found. Skipping results creation."
            )

    def cleaning(self):
        self.__log.info("[RESOURCE_E] Cleaning up experiment.")
        try:
            # Remove SCHEDULABLE label from all nodes
            self.k.node_manager.reset_scaling_labels()
            self.k.node_manager.reset_state_labels()
        except Exception as e:
            self.__log.error(
                f"[RESOURCE_E] Error cleaning up - resetting labels : {str(e)}"
            )
        try:
            self.k.statefulset_manager.reset_taskmanagers()

            jobmanager_labels = "app=flink,component=jobmanager"
            self.k.pod_manager.delete_pods_by_label(jobmanager_labels, "flink")
        except Exception as e:
            self.__log.error(
                f"[RESOURCE_E] Error cleaning up - resetting flink: {str(e)}"
            )
        try:
            self.delete_load_generators()
        except Exception as e:
            self.__log.error(
                f"[RESOURCE_E] Error cleaning up - deleting load generators: {str(e)}"
            )
        try:
            self.reload_kafka()
        except Exception as e:
            self.__log.error(
                f"[RESOURCE_E] Error cleaning up - reloading kafka: {str(e)}"
            )

    def running(self):
        self.__log.info("[RESOURCE_E] Running experiment.")
        self.join_thread()

    def __run_experiment(self):
        for core in self.cores_values:
            for memory in self.memory_values:
                self.__log.info(
                    f"[RESOURCE_E] Running experiment with {core} cores and {memory} memory."
                )
                config_dict = {
                    "tm_name": f"flink-{core}-{memory}",
                    "core": core,
                    "memory": memory,
                    "slots": 1,
                    "template_deployment": True,
                }

                self.p.reload_playbook(
                    "application/flink", config=self.config, extra_vars=config_dict
                )

                for run in range(self.runs):
                    self.__log.info(f"[RESOURCE_E] Starting run {run + 1}")
                    try:
                        # Get start timestamp of this run
                        start_ts = int(datetime.now().timestamp())
                        # Execute single run
                        ret = self.__single_run()

                        if ret == 1:
                            # Run was stopped
                            self.__log.info(f"[RESOURCE_E] Exiting run {run + 1}")
                            return 1

                        # Get end timestamp of this run
                        end_ts = int(datetime.now().timestamp())

                        # Save timestamps
                        self.timestamps.append((start_ts, end_ts))
                        self.__log.info(
                            f"[RESOURCE_E] Run {run + 1} completed. Start: {start_ts}, End: {end_ts}"
                        )
                        self.__log.info(
                            "[RESOURCE_E] Sleeping for 15 seconds before next run."
                        )
                        ret = self.current_experiment_thread.sleep(15)
                        if ret == 1:
                            self.__log.info(f"[RESOURCE_E] Exiting run {run + 1}")
                            return 1

                    except Exception as e:
                        self.__log.error(f"[RESOURCE_E] Error during run: {str(e)}")
                        return 1

    def __single_run(self):
        try:
            # Get current config
            config = self.config

            # Create scaling object
            s = Scaling(self.__log, config, self.k)
            s.set_sleep_command(self.current_experiment_thread.sleep)

            # Start load generators
            self.run_load_generators()

            # Run scaling steps on job
            ret = s.run()
            if ret == 1:
                return 1
            # Cleaup after each run
            self.cleaning()
        except Exception as e:
            self.__log.error(f"[RESOURCE_E] Error during single run: {str(e)}")
            return 1
