from scripts.monitor.experiments.Experiment import Experiment


class SimpleExperiment(Experiment):
    exp_type = "simple"

    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log

    def exp(self):
        self.do_multi_run()
