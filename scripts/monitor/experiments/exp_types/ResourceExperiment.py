from datetime import datetime

from scripts.monitor.experiments.Experiment import Experiment
from scripts.src.data.DataManager import DataManager
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Tools import FolderManager


class ResourceExperiment(Experiment):
    exp_type = "resource"

    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log
        self.config = config

        self.cpu_millis = [
            cpu * 1000
            for cpu in self.config.get_list_int(Key.Experiment.cpu_values.key)
        ]
        self.memory_values = self.config.get_list_int(Key.Experiment.memory_values.key)
        self.timestamps = {}

    def finishing(self):
        if not self.timestamps:
            self.__log.warning("[RESOURCE_E] No timestamps to export.")
            return

        f = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)
        try:
            date_folder = f.create_date_folder(self.timestamps[0][0][0])
            # Get node name from strategy
            node_type = self.config.get_str(Key.Experiment.Scaling.steps.key)[0]["node"]
            vm_type = (
                self.config.get_str(Key.Experiment.Scaling.steps.key)[0]["type"]
                if node_type == "vm_grid5000"
                else None
            )

            # set node_name to "bm" if node_type is "grid5000", "vml" if node_type is "vm_grid5000" and vm_type is "large", "vms" if node_type is "vm_grid5000" and vm_type is "small", "pico" if node_type is "pico"
            node_name = (
                "bm"
                if node_type == "grid5000"
                else "vml"
                if node_type == "vm_grid5000" and vm_type == "large"
                else "vms"
                if node_type == "vm_grid5000" and vm_type == "small"
                else "pico"
            )

            # Check if folder node_name_1 exists, if not create it
            res_exp_folder = f.create_subfolder(
                date_folder, type="res_exp", node_name=node_name
            )

            for tm_name in self.timestamps.items():
                tm_path = f.create_subfolder(res_exp_folder, type="tm", tm_name=tm_name)
                for (start_ts, end_ts) in self.timestamps[tm_name]:
                    single_run_path = f.create_subfolder(tm_path, type="single_run")
                    self.t.create_log_file(
                        self.config.to_json(), single_run_path, start_ts, end_ts
                    )

            dm = DataManager(self.__log, self.config)
            dm.export(res_exp_folder)
        except Exception as e:
            self.__log.error(f"[RESOURCE_E] Error during finishing: {str(e)}")

    def do_multi_run(self, tm_name):
        for run in range(self.runs):
            self.__log.info(f"[RESOURCE_E] Starting run {run + 1}/{self.runs}")
            self.__log.info(f"[RESOURCE_E] Using TM : {tm_name}")

            try:
                start_ts = int(datetime.now().timestamp())
                if self.__single_run() == 1:
                    self.__log.info(
                        f"[RESOURCE_E] Experiment exiting run {run + 1}/{self.runs}"
                    )
                    return 1
                end_ts = int(datetime.now().timestamp())
                self.timestamps[tm_name].append((start_ts, end_ts))
                self.__log.info(
                    f"[RESOURCE_E] Run {run + 1}/{self.runs} completed. Start: {start_ts}, End: {end_ts}"
                )

                if self.current_experiment_thread.sleep(10) == 1:
                    self.__log.info(
                        f"[RESOURCE_E] Experiment exiting run {run + 1}/{self.runs}"
                    )
                    return 1

            except Exception as e:
                self.__log.error(
                    f"[RESOURCE_E] Error in run {run + 1}/{self.runs}: {str(e)}"
                )
                raise e

    def exp(self):
        for c_val in self.cpu_millis:
            for memory in self.memory_values:
                self.__log.info(
                    f"[RESOURCE_E] Running experiment with {c_val} cores and {memory} memory."
                )

                config_dict = {
                    "tm_name": f"flink-{c_val}m-{memory}",
                    "cpu_milli": c_val,
                    "memory": memory,
                    "slots": 1,
                    "template_deployment": True,
                }

                # Create Custom Flink StatefulSet from template using (cpu_milli, memory)
                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="create",
                    quiet=True,
                )

                # Run the experiment
                self.do_multi_run(tm_name=config_dict["tm_name"])

                # Cleanup Custom Flink deployment
                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="delete",
                    quiet=True,
                )
