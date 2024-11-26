import configparser as cp
import json
import os.path
from inspect import getmembers, isclass

from scripts.utils.Defaults import (
    DefaultKeys as Key,
)
from .Logger import Logger


class Config:
    __config = {}
    RUNTIME_PATH = "/app/conf/runtime/runtime.ini"
    DEFAULTS_PATH = "/app/conf/defaults.ini"

    def __str__(self):
        return self.to_json()

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

    def get_str(self, key) -> str:
        return str(self.get(key))

    def get_list_str(self, key):
        return [str(value) for value in self.get_str(key).split(",")]

    def get_list_int(self, key):
        return [int(value) for value in self.get_str(key).split(",")]

    def __init__(self, log: Logger, _param):
        self.__log = log
        # Initialize the configuration parser
        self.cp = cp.ConfigParser()
        # Initialize default values
        self.__init_defaults()

        # Check the type of the configuration file
        if isinstance(_param, dict):
            # If the configuration is a dictionary, load it directly in self.__config
            self.__config = _param
        elif isinstance(_param, str):
            # If the configuration is a string, lets assume it is a path to a configuration file
            if not os.path.exists(_param):
                self.__log.error(
                    f"Configuration file {_param} not found or does not exist."
                )
                exit(1)
            else:
                # Now check if the file matches the .ini format
                if _param.endswith(".ini"):
                    self.cp.read(_param)
                    # Verify we only have one section
                    sections_in_file = self.cp.sections()
                    sections_in_file = set(sections_in_file) - {"scalehub"}
                    if all(
                        section.startswith("experiment") for section in sections_in_file
                    ):
                        # Platform is already provided. Performing experiment related actions
                        self.__validate_experiment(_param)

                        self.__read_experiment_config(_param)

                    elif all(
                        section.startswith("platforms") for section in sections_in_file
                    ):
                        # Check if a platform is already running.
                        self.__validate_platforms(_param)

                        self.__read_platform_config(_param)
                    else:
                        self.__log.error(
                            f"Invalid configuration file {_param}. Found {sections_in_file} sections in file. Expected either experiment or platforms section to be present in file."
                        )

                        exit(1)

                    # self.validate(_param)
                    # self.__read_config_file(_param)
                # Otherwise we might be reading a .txt log file with the configuration
                elif "log" in _param:
                    self.load_from_log(_param)
                else:
                    self.__log.error(
                        f"Invalid configuration file {_param}. Expected .ini file or log file."
                    )
                    exit(1)

        else:
            self.__log.error(
                f"Invalid type for conf: {type(_param)}. Expected path (str) or dict."
            )
            exit(1)

    def __init_defaults(self):
        # Load default values from /app/conf/defaults.ini . The only allowed section to be read is the scalehub section.
        self.cp.read(self.DEFAULTS_PATH)
        if self.cp.has_section("scalehub"):
            for key in self.cp["scalehub"]:
                dict_key = f"scalehub.{key}"
                self.__config[dict_key] = self.cp["scalehub"][key]

    def create_runtime_file(self):
        # Write the configuration to the file
        with open(self.RUNTIME_PATH, "w") as f:
            self.cp.write(f)

    def delete_runtime_file(self):
        if os.path.exists(self.RUNTIME_PATH):
            os.remove(self.RUNTIME_PATH)

    def __parse_platform(self):
        # Get platform names
        platform_str = []
        for value in self.cp[Key.Platforms.platforms].values():
            platform_str = value.split(",")
        platforms = []
        for platform_name in platform_str:
            name = platform_name.strip()

            # Get section name for platform
            platform_section = f"platforms.{name}"

            # Get values for platform
            type = self.cp[platform_section]["type"]

            # If type is RaspberryPi, Required fields are "cluster", "control", "producers", "consumers", "kubernetes_type". Otherwise, require everything else.
            if type == "RaspberryPi":
                cluster = self.cp[platform_section]["cluster"]
                producers = self.cp[platform_section]["producers"]
                consumers = self.cp[platform_section]["consumers"]
                kubernetes_type = self.cp[platform_section]["kubernetes_type"]
                control = self.cp[platform_section]["control"]
                # Create platform dictionary
                platform = {
                    "name": name,
                    "type": type,
                    "cluster": cluster,
                    "producers": int(producers),
                    "consumers": int(consumers),
                    "kubernetes_type": kubernetes_type,
                    "control": True if control.lower() == "true" else False,
                }
            # If type is FIT, Required fields are "reservation_name", "site", "archi", "producers", "consumers", "walltime". Otherwise, require everything else.
            elif type == "FIT":
                reservation_name = self.cp[platform_section]["reservation_name"]
                site = self.cp[platform_section]["site"]
                archi = self.cp[platform_section]["archi"]
                producers = self.cp[platform_section]["producers"]
                consumers = self.cp[platform_section]["consumers"]
                walltime = self.cp[platform_section]["walltime"]
                start_time = (
                    self.cp[platform_section]["start_time"]
                    if self.cp.has_option(platform_section, "start_time")
                    else None
                )
                control = self.cp[platform_section]["control"]
                # Create platform dictionary
                platform = {
                    "name": name,
                    "type": type,
                    "reservation_name": reservation_name,
                    "site": site,
                    "archi": archi,
                    "producers": int(producers),
                    "consumers": int(consumers),
                    "walltime": walltime,
                    "start_time": start_time,
                    "control": True if control.lower() == "true" else False,
                }
            elif type == "VM_on_Grid5000":
                reservation_name = self.cp[platform_section]["reservation_name"]
                site = self.cp[platform_section]["site"]
                cluster = self.cp[platform_section]["cluster"]
                producers = self.cp[platform_section]["producers"]
                consumers = self.cp[platform_section]["consumers"]
                core_per_vm = self.cp[platform_section]["core_per_vm"]
                memory_per_vm = self.cp[platform_section]["memory_per_vm"]
                disk_per_vm = self.cp[platform_section]["disk_per_vm"]
                queue = self.cp[platform_section]["queue"]
                walltime = self.cp[platform_section]["walltime"]
                start_time = (
                    self.cp[platform_section]["start_time"]
                    if self.cp.has_option(platform_section, "start_time")
                    else None
                )
                control = self.cp[platform_section]["control"]
                # Create platform dictionary
                platform = {
                    "name": name,
                    "type": type,
                    "reservation_name": reservation_name,
                    "site": site,
                    "cluster": cluster,
                    "producers": int(producers),
                    "consumers": int(consumers),
                    "core_per_vm": int(core_per_vm),
                    "memory_per_vm": int(memory_per_vm),
                    "disk_per_vm": int(disk_per_vm),
                    "queue": queue,
                    "walltime": walltime,
                    "start_time": start_time,
                    "control": True if control.lower() == "true" else False,
                }

            else:
                reservation_name = self.cp[platform_section]["reservation_name"]
                site = self.cp[platform_section]["site"]
                cluster = self.cp[platform_section]["cluster"]
                producers = self.cp[platform_section]["producers"]
                consumers = self.cp[platform_section]["consumers"]
                queue = self.cp[platform_section]["queue"]
                walltime = self.cp[platform_section]["walltime"]
                kubernetes_type = self.cp[platform_section]["kubernetes_type"]
                control = self.cp[platform_section]["control"]
                start_time = (
                    self.cp[platform_section]["start_time"]
                    if self.cp.has_option(platform_section, "start_time")
                    else None
                )
                # Create platform dictionary
                platform = {
                    "name": name,
                    "type": type,
                    "reservation_name": reservation_name,
                    "site": site,
                    "cluster": cluster,
                    "producers": int(producers),
                    "consumers": int(consumers),
                    "kubernetes_type": kubernetes_type,
                    "control": True if control.lower() == "true" else False,
                    "queue": queue,
                    "walltime": walltime,
                    "start_time": start_time,
                }
            # Add platform to list
            platforms.append(platform)
        return platforms

    def __parse_load_generators(self):
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
            lg_type = self.cp[generator_section]["type"]
            num_sensors = self.cp[generator_section]["num_sensors"]
            interval_ms = self.cp[generator_section]["interval_ms"]
            replicas = self.cp[generator_section]["replicas"]
            value = self.cp[generator_section]["value"]

            # Create generator dictionary
            generator = {
                "name": name,
                "type": lg_type,
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
            # Skip private attributes, the Generators class in this step
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

    def __validate_platforms(self, conf_path: str):
        # Validate the Platform sections
        platform_str = []
        for value in self.cp[Key.Platforms.platforms].values():
            platform_str = value.split(",")
        for platform_name in platform_str:
            name = platform_name.strip()
            platform_section = f"platforms.{name}"

            if not self.cp.has_section(platform_section):
                self.__log.error(
                    f"[CONF] Section [{platform_section}] is missing in configuration file {conf_path}"
                )
                exit(1)
            else:
                # Get values for platform
                type = self.cp[platform_section]["type"]

                # If type is RaspberryPi, Required fields are "cluster", "producers", "consumers", "kubernetes_type". Otherwise, require everything else.
                if type == "RaspberryPi":
                    required_keys = [
                        "cluster",
                        "control",
                        "producers",
                        "consumers",
                        "kubernetes_type",
                    ]
                # If type is FIT, Required fields are "reservation_name", "site", "archi", "producers", "consumers", "control". Otherwise, require everything else.
                elif type == "FIT":
                    required_keys = [
                        "reservation_name",
                        "control",
                        "site",
                        "archi",
                        "producers",
                        "consumers",
                    ]
                elif type == "VM_on_Grid5000":
                    required_keys = [
                        "reservation_name",
                        "control",
                        "site",
                        "cluster",
                        "producers",
                        "consumers",
                        "core_per_vm",
                        "memory_per_vm",
                        "queue",
                        "walltime",
                    ]
                else:
                    required_keys = [
                        "reservation_name",
                        "control",
                        "site",
                        "cluster",
                        "producers",
                        "consumers",
                        "queue",
                        "walltime",
                    ]

                # Check that all keys are defined in the configuration file for this section
                for key in required_keys:
                    if not self.cp.has_option(platform_section, key):
                        self.__log.error(
                            f"[CONF] Key [{key}] is missing in section [{platform_section}] in configuration file {conf_path}"
                        )
                        exit(1)

    # def validate(self, conf_path: str):
    #     # Check that the configuration file exists
    #     if not exists(conf_path):
    #         self.__log.error(f"[CONF] Config file [{conf_path}] does not exist.")
    #         exit(1)
    #     else:
    #         self.__log.debugg(f"[CONF] Config file [{conf_path}] found.")
    #
    #         # Read the configuration file
    #         self.cp.read(conf_path)
    #
    #         # Check that the main sections are defined
    #         for name, cls in getmembers(Key, isclass):
    #             if name.startswith("__") and name.endswith("__"):
    #                 continue
    #             section = name.lower()
    #             if not self.cp.has_section(section):
    #                 self.__log.error(
    #                     f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
    #                 )
    #                 exit(1)
    #         # Validate subclasses of Experiment
    #         self.__validate_experiment(conf_path)

    def __read_platform_config(self, conf_path: str):
        # Read the configuration file
        self.cp.read(conf_path)

        for section in self.cp.sections():
            for key in self.cp[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = self.cp[section][key]

        self.__config[Key.Platforms.platforms] = self.__parse_platform()

    def __read_experiment_config(self, conf_path: str):
        # Read the configuration file
        self.cp.read(conf_path)

        for section in self.cp.sections():
            for key in self.cp[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = self.cp[section][key]

        self.__config[
            Key.Experiment.Generators.generators
        ] = self.__parse_load_generators()

    # Serialize the configuration to a JSON string
    def to_json(self):
        return json.dumps(self.__config, indent=4)

    def load_from_log(self, log_path: str):
        with open(log_path, "r") as f:
            lines = f.readlines()

        # Find the line number where [CONFIG] starts
        start_line = lines.index("[CONFIG]\n") + 1

        # Find the line number where [TIMESTAMPS] starts
        end_line = lines.index("[TIMESTAMPS]\n")

        # Join the lines between [CONFIG] and [TIMESTAMPS] and load as JSON
        config_content = "".join(lines[start_line:end_line])
        self.__config = json.loads(config_content)
