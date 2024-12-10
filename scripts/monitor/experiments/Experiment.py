import os
import threading

from scripts.src.resources.FlinkManager import FlinkManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class StoppableThread(threading.Thread):
    def __init__(self, log, target=None, *args, **kwargs):
        self.__stop_event = threading.Event()
        self.__log = log
        self.target = target
        super(StoppableThread, self).__init__(*args, **kwargs)

    def stop_thread(self):
        self.__log.info("[THRD] Stopping thread.")
        self.__stop_event.set()
        self.join()

    def stopped(self):
        return self.__stop_event.is_set()

    def run(self):
        self.__log.info("[THRD] Starting thread.")
        self.target()

    def join(self, timeout=None):
        super(StoppableThread, self).join(timeout)


class Experiment:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"
    TEMPLATES_BASE_PATH = "/app/templates"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k: KubernetesManager = KubernetesManager(log)
        self.t: Tools = Tools(log)
        self.f: FlinkManager = FlinkManager(log, config)

        self.current_experiment_thread = None

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

    def start_thread(self, target):
        self.current_experiment_thread = StoppableThread(log=self.__log, target=target)
        self.current_experiment_thread.start()

    def stop_thread(self):
        if self.current_experiment_thread:
            self.current_experiment_thread.stop_thread()

    def join_thread(self):
        if self.current_experiment_thread:
            self.current_experiment_thread.join()

    def starting(self):
        raise NotImplementedError("Starting method not implemented.")

    def finishing(self):
        raise NotImplementedError("Finishing method not implemented.")

    def cleaning(self):
        raise NotImplementedError("Cleaning method not implemented.")

    def running(self):
        raise NotImplementedError("Running method not implemented.")
