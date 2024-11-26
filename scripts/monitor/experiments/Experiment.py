import os

from scripts.utils.Config import Config
from scripts.utils.KubernetesManager import KubernetesManager
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

        # Dump experiment information to log file
        with open(log_file_path, "w") as file:
            file.write(f"[CONFIG]\n")
            file.write(f"{self.config.to_json()}\n\n")
            file.write(f"[TIMESTAMPS]\n")
            file.write(f"Experiment start at : {start_ts}\n")
            file.write(f"Experiment end at : {end_ts}\n")
        return log_file_path

    def start(self):
        raise NotImplementedError("Start method not implemented.")

    def monitor(self):
        raise NotImplementedError("Monitor method not implemented.")

    def stop(self):
        raise NotImplementedError("Stop method not implemented.")

    def cleanup(self):
        raise NotImplementedError("Cleanup method not implemented.")

    def running(self):
        raise NotImplementedError("Running method not implemented.")
