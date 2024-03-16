import configparser as cp
import json
from inspect import getmembers, isclass
from os.path import exists

from scripts.utils.Defaults import (
    DefaultValues as Value,
    DefaultKeys as Key,
)
from .Logger import Logger


class Config:
    __config = {}

    def __init__(self, log: Logger, conf):
        self.__log = log
        # Initialize default values
        self.__init_defaults()

        # Initialize the configuration parser
        self.cp = cp.ConfigParser()

        # Check the type of the configuration file
        if isinstance(conf, dict):
            # If the configuration is a dictionary, load it directly in self.__config
            self.__config = conf
        elif isinstance(conf, str):
            # If the configuration is a string, lets assume it is a path to a configuration file

            # Now check if the file matches the .ini format
            if conf.endswith(".ini"):
                self.validate(conf)
                self.__read_config_file(conf)
            # Otherwise we might be reading a .txt log file with the configuration
            elif "log" in conf:
                self.load_from_log(conf)
        else:
            self.__log.error(
                f"Invalid type for conf: {type(conf)}. Expected path (str) or dict."
            )
            exit(1)

    def __init_defaults(self):
        self.__config[Key.Scalehub.playbook] = Value.Scalehub.playbooks
        self.__config[Key.Scalehub.inventory] = Value.Scalehub.inventory
        self.__config[Key.Scalehub.experiments] = Value.Scalehub.experiments
        self.__config[Key.Scalehub.debug_level] = Value.Scalehub.Debug.level

        self.__config[Key.Platform.type] = Value.Platform.type
        self.__config[Key.Platform.reservation_name] = Value.Platform.reservation_name
        self.__config[Key.Platform.site] = Value.Platform.site
        self.__config[Key.Platform.cluster] = Value.Platform.cluster
        self.__config[Key.Platform.producers] = Value.Platform.producers
        self.__config[Key.Platform.consumers] = Value.Platform.consumers
        self.__config[Key.Platform.queue] = Value.Platform.queue
        self.__config[Key.Platform.walltime] = Value.Platform.walltime
        self.__config[Key.Platform.kubernetes_type] = Value.Platform.kubernetes_type
        self.__config[Key.Platform.start_time] = Value.Platform.start_time

        self.__config[Key.Experiment.name] = Value.Experiment.name
        self.__config[Key.Experiment.job_file] = Value.Experiment.job_file
        self.__config[Key.Experiment.task_name] = Value.Experiment.task_name
        self.__config[Key.Experiment.output_skip_s] = Value.Experiment.output_skip_s
        self.__config[Key.Experiment.output_plot] = Value.Experiment.output_plot
        self.__config[Key.Experiment.output_stats] = Value.Experiment.output_stats
        self.__config[
            Key.Experiment.broker_mqtt_host
        ] = Value.Experiment.broker_mqtt_host
        self.__config[
            Key.Experiment.broker_mqtt_port
        ] = Value.Experiment.broker_mqtt_port
        self.__config[
            Key.Experiment.kafka_partitions
        ] = Value.Experiment.kafka_partitions

        self.__config[Key.Experiment.Chaos.enable] = Value.Experiment.Chaos.enable

        self.__config[
            Key.Experiment.Chaos.delay_latency_ms
        ] = Value.Experiment.Chaos.latency_ms
        self.__config[
            Key.Experiment.Chaos.delay_jitter_ms
        ] = Value.Experiment.Chaos.jitter_ms
        self.__config[
            Key.Experiment.Chaos.delay_correlation
        ] = Value.Experiment.Chaos.correlation

        self.__config[
            Key.Experiment.Chaos.bandwidth_rate_mbps
        ] = Value.Experiment.Chaos.bandwidth_rate_mbps

        self.__config[
            Key.Experiment.Chaos.bandwidth_limit
        ] = Value.Experiment.Chaos.bandwidth_limit

        self.__config[
            Key.Experiment.Chaos.bandwidth_buffer
        ] = Value.Experiment.Chaos.bandwidth_buffer

        self.__config[
            Key.Experiment.Generators.generators
        ] = Value.Experiment.Generators

        self.__config[
            Key.Experiment.Transscale.max_parallelism
        ] = Value.Experiment.Transscale.max_parallelism

        self.__config[
            Key.Experiment.Transscale.monitoring_warmup_s
        ] = Value.Experiment.Transscale.monitoring_warmup_s
        self.__config[
            Key.Experiment.Transscale.monitoring_interval_s
        ] = Value.Experiment.Transscale.monitoring_interval_s

        self.__config[
            Key.Experiment.Flink.checkpoint_interval_ms
        ] = Value.Experiment.Flink.checkpoint_interval_ms
        self.__config[
            Key.Experiment.Flink.window_size_ms
        ] = Value.Experiment.Flink.window_size_ms
        self.__config[
            Key.Experiment.Flink.fibonacci_value
        ] = Value.Experiment.Flink.fibonacci_value

    def get(self, key) -> any:
        if key in self.__config:
            return self.__config[key]

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_bool(self, key) -> bool:
        if self.get_str(key).lower() == "true":
            return True
        else:
            return False

    def get_float(self, key) -> float:
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def get_list_str(self, key):
        return [str(value) for value in self.get_str(key).split(",")]

    def get_list_int(self, key):
        return [int(value) for value in self.get_str(key).split(",")]

    def parse_load_generators(self):
        # Get load_generators names
        load_generators_str = []
        for value in self.cp[Key.Experiment.Generators.generators].values():
            load_generators_str = value.split(",")
        load_generators = []
        for generator_name in load_generators_str:
            name = generator_name.strip()
            # Get section name for generator
            generator_section = f"experiment.generators.{name}"

            # Get values for generator
            topic = self.cp[generator_section]["topic"]
            num_sensors = self.cp[generator_section]["num_sensors"]
            interval_ms = self.cp[generator_section]["interval_ms"]
            replicas = self.cp[generator_section]["replicas"]
            value = self.cp[generator_section]["value"]

            # Create generator dictionary
            generator = {
                "name": name,
                "topic": topic,
                "num_sensors": num_sensors,
                "interval_ms": interval_ms,
                "replicas": replicas,
                "value": value,
            }
            # Add generator to list
            load_generators.append(generator)
        return load_generators

    def __validate_experiment(self, conf_path: str):
        section_base = "experiment"
        # Validate subclasses of Experiment
        for name, cls in getmembers(Key.Experiment, isclass):
            # Skip private attributes and the Generators class in this step
            if name.startswith("__") and name.endswith("__") or name == "Generators":
                continue
            section = f"{section_base}.{name.lower()}"
            if not self.cp.has_section(section):
                self.__log.error(
                    f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
                )
                exit(1)
            else:
                # For each subclass of cls get keys and filter out the private ones.
                keys = [
                    key
                    for key, value in getmembers(cls)
                    if not key.startswith("__") and not key.endswith("__")
                ]

                # Check that all keys are defined in the configuration file for this section
                for key in keys:
                    if not self.cp.has_option(section, key):
                        self.__log.error(
                            f"[CONF] Key [{key}] is missing in section [{section}] in configuration file {conf_path}"
                        )
                        exit(1)
        # Validate the Generators class
        section = f"{section_base}.generators"
        if not self.cp.has_section(section):
            self.__log.error(
                f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
            )
            exit(1)
        else:
            # Get the list of generators defined in the configuration file; this is a comma separated list
            generators = self.cp[section]["generators"].split(",")
            # For each generator, check that the section exists
            for generator in generators:
                section = f"{section_base}.generators.{generator.strip()}"
                if not self.cp.has_section(section):
                    self.__log.error(
                        f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
                    )
                    exit(1)
                else:
                    # For each subclass of cls get keys and filter out the private ones.
                    keys = [
                        key
                        for key, value in getmembers(
                            Key.Experiment.Generators.Generator
                        )
                        if not key.startswith("__") and not key.endswith("__")
                    ]

                    # Check that all keys are defined in the configuration file for this section
                    for key in keys:
                        if not self.cp.has_option(section, key):
                            self.__log.error(
                                f"[CONF] Key [{key}] is missing in section [{section}] in configuration file {conf_path}"
                            )
                            exit(1)

    def validate(self, conf_path: str):
        # Check that the configuration file exists
        if not exists(conf_path):
            self.__log.error(f"[CONF] Config file [{conf_path}] does not exist.")
            exit(1)
        else:
            self.__log.debugg(f"[CONF] Config file [{conf_path}] found.")

            # Read the configuration file
            self.cp.read(conf_path)

            # Check that the main sections are defined
            for name, cls in getmembers(Key, isclass):
                if name.startswith("__") and name.endswith("__"):
                    continue
                section = name.lower()
                if not self.cp.has_section(section):
                    self.__log.error(
                        f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
                    )
                    exit(1)
            # Validate subclasses of Experiment
            self.__validate_experiment(conf_path)

    def __read_config_file(self, conf_path: str):
        # Read the configuration file
        self.cp.read(conf_path)

        for section in self.cp.sections():
            for key in self.cp[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = self.cp[section][key]

        self.__config[
            Key.Experiment.Generators.generators
        ] = self.parse_load_generators()

    # Serialize the configuration to a JSON string
    def to_json(self):
        return json.dumps(self.__config)

    def load_from_log(self, log_path: str):
        with open(log_path, "r") as f:
            lines = f.readlines()

        # Find the line number where [CONFIG] starts
        start_line = lines.index("[CONFIG]\n") + 1

        # Find the line number where [TIMESTAMPS] starts
        end_line = lines.index("[TIMESTAMPS]\n")

        # Join the lines between [CONFIG] and [TIMESTAMPS] and load as JSON
        config_content = "".join(lines[start_line:end_line])
        self.__conf = json.loads(config_content)
