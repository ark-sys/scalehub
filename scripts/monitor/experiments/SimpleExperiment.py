from datetime import datetime

from scripts.monitor.experiments.Experiment import Experiment
from scripts.utils.Defaults import DefaultKeys as Key


class SimpleExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log

        self.runs = self.config.get_int(Key.Experiment.runs)

    def starting(self):
        self.__log.info("[SIMPLE_E] Starting experiment.")

        # Create a new thread for the experiment
        self.start_thread(self.__run_experiment)

    def __run_experiment(self):
        for run in range(self.runs):
            self.__log.info(f"[SIMPLE_E] Starting run {run + 1}")
            try:
                # Get start timestamp of this run
                start_ts = int(datetime.now().timestamp())
                # Execute single run
                ret = self.single_run()

                if ret == 1:
                    # Run was stopped
                    self.__log.info(f"[SIMPLE_E] Exiting run {run + 1}")
                    break

                # Get end timestamp of this run
                end_ts = int(datetime.now().timestamp())

                # Save timestamps
                self.timestamps.append((start_ts, end_ts))
                self.__log.info(
                    f"[SIMPLE_E] Run {run + 1} completed. Start: {start_ts}, End: {end_ts}"
                )
                self.__log.info("[SIMPLE_E] Sleeping for 15 seconds before next run.")
                ret = self.current_experiment_thread.sleep(15)
                if ret == 1:
                    self.__log.info(f"[SIMPLE_E] Exiting run {run + 1}")
                    break

            except Exception as e:
                self.__log.error(f"[SIMPLE_E] Error during run: {str(e)}")
                break
