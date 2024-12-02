import os

from scripts.monitor.experiments.Scaling import Scaling
from scripts.src.resources.FlinkManager import FlinkManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class Experiment:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"
    TEMPLATES_BASE_PATH = "/app/templates"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k: KubernetesManager = KubernetesManager(log)
        self.t: Tools = Tools(log)
        self.f: FlinkManager = FlinkManager(log, config)
        self.s: Scaling = Scaling(log, config)

        # Chaos resources templates
        self.consul_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/consul-latency.yaml.j2"
        )
        self.flink_chaos_template = f"{self.TEMPLATES_BASE_PATH}/flink-latency.yaml.j2"
        self.storage_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/storage-latency.yaml.j2"
        )
        self.load_generator_deployment_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-deployment.yaml.j2"
        )
        self.load_generator_service_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-service.yaml.j2"
        )

    def create_log_file(self, exp_path, start_ts, end_ts):
        # Create log file
        log_file_path = os.path.join(exp_path, "exp_log.txt")
        try:
            # Dump experiment information to log file
            with open(log_file_path, "w") as file:
                file.write(f"[CONFIG]\n")
                file.write(f"{self.config.to_json()}\n\n")
                file.write(f"[TIMESTAMPS]\n")
                file.write(f"Experiment start at : {start_ts}\n")
                file.write(f"Experiment end at : {end_ts}\n")
            return log_file_path
        except Exception as e:
            self.__log.error(f"[EXP] Error creating log file: {e}")

    def run_load_generators(self):
        # Create load generators
        for generator in self.config.get(Key.Experiment.Generators.generators):
            try:
                load_generator_params = {
                    "theodolite_lg_image": "registry.gitlab.inria.fr/stream-processing-autoscaling/scalehub/workload-generator",
                    "theodolite_lg_tag": "latest",
                    "lg_name": generator["name"],
                    "lg_topic": generator["topic"],
                    "lg_numsensors": int(generator["num_sensors"]),
                    "lg_intervalms": int(generator["interval_ms"]),
                    "lg_replicas": int(generator["replicas"]),
                    "lg_value": int(generator["value"]),
                }
                self.k.service_manager.create_service_from_template(
                    self.load_generator_service_template, load_generator_params
                )
                self.k.deployment_manager.create_deployment_from_template(
                    self.load_generator_deployment_template, load_generator_params
                )
            except Exception as e:
                self.__log.error(f"[EXP] Error creating load generator: {e}")

    def delete_load_generators(self):
        # Delete load generators
        for generator in self.config.get(Key.Experiment.Generators.generators):
            try:
                load_generator_params = {
                    "theodolite_lg_image": "registry.gitlab.inria.fr/stream-processing-autoscaling/scalehub/workload-generator",
                    "theodolite_lg_tag": "latest",
                    "lg_name": generator["name"],
                    "lg_topic": generator["topic"],
                    "lg_numsensors": int(generator["num_sensors"]),
                    "lg_intervalms": int(generator["interval_ms"]),
                    "lg_replicas": int(generator["replicas"]),
                    "lg_value": int(generator["value"]),
                }
                self.k.service_manager.delete_service_from_template(
                    self.load_generator_service_template, load_generator_params
                )
                self.k.deployment_manager.delete_deployment_from_template(
                    self.load_generator_deployment_template, load_generator_params
                )
            except Exception as e:
                self.__log.error(f"[EXP] Error deleting load generator: {e}")

    def init_cluster(self):
        try:
            # Check list of schedulable node, we should have 0
            schedulable_nodes = self.k.node_manager.get_schedulable_nodes()
            # Reset scaling labels, clean start.
            self.k.node_manager.reset_scaling_labels()
            self.__log.info(f"[EXP] Schedulable nodes: {len(schedulable_nodes)}")
            # Reset state labels
            self.k.node_manager.reset_state_labels()
            # Reset all taskmanagers to 0 replicas
            self.k.statefulset_manager.reset_taskmanagers()
        except Exception as e:
            self.__log.error(f"[EXP] Error initializing cluster: {e}")

    def is_chaos_enabled(self):
        return self.config.get_bool("chaos.enabled")

    def start(self):
        raise NotImplementedError("Start method not implemented.")

    def stop(self):
        raise NotImplementedError("Stop method not implemented.")

    def cleanup(self):
        raise NotImplementedError("Cleanup method not implemented.")

    def running(self):
        raise NotImplementedError("Running method not implemented.")
