from scripts.monitor.experiments.Experiment import Experiment


class StandaloneExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)

    def start(self):
        pass

    def monitor(self):
        pass

    def stop(self):
        pass

    def running(self):
        self.log.info("Running standalone experiment.")

        while True:
            pass
