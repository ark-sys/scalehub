from scripts.monitor.experiments.Experiment import Experiment
from scripts.utils.Defaults import DefaultKeys as Key


class ResourceExperiment(Experiment):
    exp_type = "resource"

    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log
        self.config = config

        self.cpu_millis = [
            str(cpu * 1000) + "m"
            for cpu in self.config.get_list_int(Key.Experiment.cpu_values.key)
        ]
        self.memory_values = self.config.get_list_int(Key.Experiment.memory_values.key)

    def exp(self):
        for c_val in self.cpu_millis:
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

                # Create Custom Flink StatefulSet from template using (cpu_milli, memory)
                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="create",
                    quiet=True,
                )

                # Run the experiment
                self.do_multi_run()

                # Cleanup Custom Flink deployment
                self.p.run(
                    "application/flink",
                    config=self.config,
                    extra_vars=config_dict,
                    tag="delete",
                    quiet=True,
                )
