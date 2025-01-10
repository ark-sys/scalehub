from datetime import datetime

from scripts.monitor.experiments.Experiment import Experiment
from scripts.utils.Defaults import DefaultKeys as Key


class ResourceExperiment(Experiment):
    memory_values = [1024, 2048, 4096, 8192, 16384, 32768]
    cpu_milli = ["1000m", "2000m", "4000m", "8000m", "16000m", "32000m"]

    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log
        self.runs = self.config.get_int(Key.Experiment.runs)

    def starting(self):
        self.__log.info("[RESOURCE_E] Starting experiment.")
        self.start_thread(self.__run_experiment)

    def __run_experiment(self):
        for c_val in self.cpu_milli:
            for memory in self.memory_values:
                self.__log.info(
                    f"[RESOURCE_E] Running experiment with {c_val} cores and {memory} memory."
                )
                config_dict = {
                    "tm_name": f"flink-{c_val}-{memory}",
                    "cpu_milli": c_val,
                    "memory": memory,
                    "slots": 1,
                    "template_deployment": True,
                }

                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="create",
                    quiet=True,
                )

                for run in range(self.runs):
                    self.__log.info(
                        f"[RESOURCE_E] =================================== Starting run {run + 1} ==================================="
                    )
                    try:
                        # Get start timestamp of this run
                        start_ts = int(datetime.now().timestamp())
                        # Execute single run
                        ret = self.single_run()

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
                # Cleanup Custom Flink Deployment
                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="delete",
                    quiet=True,
                )
