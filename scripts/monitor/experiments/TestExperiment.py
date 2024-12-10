from time import sleep

from scripts.monitor.experiments.Experiment import Experiment


class TestExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log

    def starting(self):
        self.__log.info("[TEST_E] Starting experiment.")
        # Create a new thread for the experiment
        self.start_thread(self._do_some_running)

    def finishing(self):
        self.__log.info("[TEST_E] Finishing experiment.")
        sleep(10)
        # Export data here

    def cleaning(self):
        self.__log.info("[TEST_E] Cleaning experiment.")
        sleep(10)
        # Clean up resources here

    def running(self):
        self.__log.info("[TEST_E] Running experiment.")
        # Wait for the thread to finish
        self.join_thread()

    def _do_some_running(self):
        self.__log.info("[TEST_E] Doing some running.")
        sleep_time = 60
        for i in range(sleep_time):
            if self.current_experiment_thread.stopped():
                self.__log.info("[TEST_E] Stopped running.")
                break
            sleep(1)
        self.__log.info("[TEST_E] Finished running.")
