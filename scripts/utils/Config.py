import configparser as cp
import json
import os.path
from inspect import getmembers, isclass

import yaml

from scripts.utils.Defaults import DefaultKeys as Key
from .Logger import Logger


class Config:
    __config = {}
    RUNTIME_PATH = "/app/conf/runtime/runtime.ini"
    DEFAULTS_PATH = "/app/conf/defaults.ini"

    def __str__(self):
        return self.to_json()

    def get(self, key) -> any:
        return self.__config.get(key)

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_bool(self, key) -> bool:
        return self.get_str(key).lower() == "true"

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
        self.cp = cp.ConfigParser()
        self.__init_defaults()

        if isinstance(_param, dict):
            self.__config = _param
        elif isinstance(_param, str):
            if not os.path.exists(_param):
                self.__log.error(
                    f"Configuration file {_param} not found or does not exist."
                )
                exit(1)
            else:
                if _param.endswith(".ini"):
                    self.cp.read(_param)
                    sections_in_file = set(self.cp.sections()) - {"scalehub"}
                    if any(
                        section.startswith("experiment") for section in sections_in_file
                    ):
                        self.__validate_and_read_experiment(_param)
                    if any(
                        section.startswith("platforms") for section in sections_in_file
                    ):
                        self.__validate_and_read_platforms(_param)
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
        self.cp.read(self.DEFAULTS_PATH)
        if self.cp.has_section("scalehub"):
            for key in self.cp["scalehub"]:
                dict_key = f"scalehub.{key}"
                self.__config[dict_key] = self.cp["scalehub"][key]

    def create_runtime_file(self):
        with open(self.RUNTIME_PATH, "w") as f:
            self.cp.write(f)

    def delete_runtime_file(self):
        if os.path.exists(self.RUNTIME_PATH):
            os.remove(self.RUNTIME_PATH)

    def __validate_and_read_experiment(self, conf_path: str):
        self.cp.read(conf_path)
        section_base = "experiment"
        experiment_type = self.cp.get(section_base, "type", fallback=None)

        for name, cls in getmembers(Key.Experiment, isclass):
            if (
                name.startswith("__")
                and name.endswith("__")
                or name in ["Generators", "Scaling"]
            ):
                continue
            if name == "Transscale" and experiment_type != "transscale":
                continue
            section = f"{section_base}.{name.lower()}"
            if not self.cp.has_section(section):
                self.__log.error(
                    f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
                )
                exit(1)
            keys = [
                key
                for key, value in getmembers(cls)
                if not key.startswith("__") and not key.endswith("__")
            ]
            for key in keys:
                if not self.cp.has_option(section, key):
                    self.__log.error(
                        f"[CONF] Key [{key}] is missing in section [{section}] in configuration file {conf_path}"
                    )
                    exit(1)
            for key in self.cp[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = self.cp[section][key]

        extra_sections = ["scaling", "generators"]
        for extra_section in extra_sections:
            section = f"{section_base}.{extra_section}"
            if not self.cp.has_section(section):
                self.__log.error(
                    f"[CONF] Section [{section}] is missing in configuration file {conf_path}"
                )
                exit(1)
            for key in self.cp[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = self.cp[section][key]

        self.__config[
            Key.Experiment.Generators.generators
        ] = self.__parse_load_generators()
        self.__config[Key.Experiment.Scaling.steps] = self.__parse_scaling_strategy()
        self.__config[Key.Experiment.Scaling.interval_scaling_s] = self.get_int(
            Key.Experiment.Scaling.interval_scaling_s
        )
        self.__config[Key.Experiment.Scaling.max_parallelism] = self.get_int(
            Key.Experiment.Scaling.max_parallelism
        )

    def __validate_and_read_platforms(self, conf_path: str):
        self.cp.read(conf_path)
        platform_str = self.cp[Key.Platforms.platforms].values()
        platforms = []
        for value in platform_str:
            for platform_name in value.split(","):
                name = platform_name.strip()
                platform_section = f"platforms.{name}"
                if not self.cp.has_section(platform_section):
                    self.__log.error(
                        f"[CONF] Section [{platform_section}] is missing in configuration file {conf_path}"
                    )
                    exit(1)
                type = self.cp[platform_section]["type"]
                platform = {"name": name, "type": type}

                base_keys = [
                    "producers",
                    "consumers",
                    "control",
                ]
                enos_keys = [
                    "reservation_name",
                    "site",
                    "cluster",
                    "queue",
                    "walltime",
                    "start_time",
                ]
                match type:
                    case "RaspberryPi":
                        extra_keys = ["kubernetes_type"]
                    case "FIT":
                        extra_keys = enos_keys + ["archi"]
                    case "VM_on_Grid5000":
                        extra_keys = enos_keys + [
                            "core_per_vm",
                            "memory_per_vm",
                            "disk_per_vm",
                        ]
                    case "Grid5000":
                        extra_keys = enos_keys
                    case _:
                        extra_keys = ["kubernetes_type"]
                keys = base_keys + extra_keys
                for key in keys:
                    if not self.cp.has_option(platform_section, key):
                        self.__log.error(
                            f"[CONF] Key [{key}] is missing in section [{platform_section}] in configuration file {conf_path}"
                        )
                        exit(1)
                    platform[key] = self.cp[platform_section][key]
                    if key in [
                        "producers",
                        "consumers",
                        "core_per_vm",
                        "memory_per_vm",
                        "disk_per_vm",
                    ]:
                        platform[key] = int(platform[key])
                    elif key == "control":
                        platform[key] = platform[key].lower() == "true"
                platforms.append(platform)
                for key in self.cp[platform_section]:
                    dict_key = f"{platform_section}.{key}"
                    self.__config[dict_key] = self.cp[platform_section][key]
        self.__config[Key.Platforms.platforms] = platforms

    def __parse_load_generators(self):
        load_generators_str = self.cp[Key.Experiment.Generators.generators].values()
        load_generators = []
        for value in load_generators_str:
            for generator_name in value.split(","):
                name = generator_name.strip()
                generator_section = f"experiment.generators.{name}"
                generator = {
                    "name": name,
                    "type": self.cp[generator_section]["type"],
                    "topic": self.cp[generator_section]["topic"],
                    "num_sensors": int(self.cp[generator_section]["num_sensors"]),
                    "interval_ms": int(self.cp[generator_section]["interval_ms"]),
                    "replicas": int(self.cp[generator_section]["replicas"]),
                    "value": int(self.cp[generator_section]["value"]),
                }
                load_generators.append(generator)
        return load_generators

    def __parse_scaling_strategy(self):
        strategy_path = self.get_str(Key.Experiment.Scaling.strategy_path)
        with open(strategy_path, "r") as file:
            strategy = yaml.safe_load(file)
        return strategy

    def to_json(self):
        return json.dumps(self.__config, indent=4)

    def load_from_log(self, log_path: str):
        with open(log_path, "r") as f:
            lines = f.readlines()
        start_line = lines.index("[CONFIG]\n") + 1
        end_line = lines.index("[TIMESTAMPS]\n")
        config_content = "".join(lines[start_line:end_line])
        self.__config = json.loads(config_content)
