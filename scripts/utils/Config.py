import configparser as cp
import json
import os
from inspect import getmembers, isclass

import yaml

from scripts.utils.Defaults import DefaultKeys as Key
from .Logger import Logger


class Config:
    RUNTIME_PATH = "/app/conf/runtime/runtime.ini"
    DEFAULTS_PATH = "/app/conf/defaults.ini"

    def __init__(self, log: Logger, _param):
        self.__log = log
        self.__config = {}
        self.cp = cp.ConfigParser()
        self.__init_defaults()

        if isinstance(_param, dict):
            self.__config = _param
        elif isinstance(_param, str):
            if not os.path.exists(_param):
                self.__log.error(f"Configuration file {_param} not found.")
                exit(1)
            elif _param.endswith(".ini"):
                self.cp.read(_param)
                sections = set(self.cp.sections()) - {"scalehub"}
                if all(section.startswith("experiment") for section in sections):
                    self.__validate_experiment(_param)
                    self.__read_experiment_config(_param)
                elif all(section.startswith("platforms") for section in sections):
                    self.__validate_platforms(_param)
                    self.__read_platform_config(_param)
                else:
                    self.__log.error(f"Invalid configuration file {_param}.")
                    exit(1)
            elif "log" in _param:
                self.load_from_log(_param)
            else:
                self.__log.error(f"Invalid configuration file {_param}.")
                exit(1)
        else:
            self.__log.error(f"Invalid type for conf: {type(_param)}.")
            exit(1)

    def __init_defaults(self):
        self.cp.read(self.DEFAULTS_PATH)
        if self.cp.has_section("scalehub"):
            for key in self.cp["scalehub"]:
                self.__config[f"scalehub.{key}"] = self.cp["scalehub"][key]

    def __validate_experiment(self, conf_path: str):
        for name, cls in getmembers(Key.Experiment, isclass):
            if name.startswith("__") or name == "Generators" or name == "Scaling":
                continue
            section = f"experiment.{name.lower()}"
            if not self.cp.has_section(section):
                self.__log.error(f"Section [{section}] is missing in {conf_path}")
                exit(1)
            for key in [key for key, _ in getmembers(cls) if not key.startswith("__")]:
                if not self.cp.has_option(section, key):
                    self.__log.error(f"Key [{key}] is missing in section [{section}]")
                    exit(1)
        self.__validate_section("experiment.scaling", conf_path)
        self.__validate_section("experiment.generators", conf_path)
        for generator in self.cp["experiment.generators"]["generators"].split(","):
            self.__validate_section(
                f"experiment.generators.{generator.strip()}", conf_path
            )

    def __validate_section(self, section, conf_path):
        if not self.cp.has_section(section):
            self.__log.error(f"Section [{section}] is missing in {conf_path}")
            exit(1)

    def __validate_platforms(self, conf_path: str):
        for platform_name in self.cp[Key.Platforms.platforms].values():
            section = f"platforms.{platform_name.strip()}"
            if not self.cp.has_section(section):
                self.__log.error(f"Section [{section}] is missing in {conf_path}")
                exit(1)
            required_keys = self.__get_required_keys(self.cp[section]["type"])
            for key in required_keys:
                if not self.cp.has_option(section, key):
                    self.__log.error(f"Key [{key}] is missing in section [{section}]")
                    exit(1)

    def __get_required_keys(self, type):
        if type == "RaspberryPi":
            return ["cluster", "control", "producers", "consumers", "kubernetes_type"]
        elif type == "FIT":
            return [
                "reservation_name",
                "control",
                "site",
                "archi",
                "producers",
                "consumers",
            ]
        elif type == "VM_on_Grid5000":
            return [
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
            return [
                "reservation_name",
                "control",
                "site",
                "cluster",
                "producers",
                "consumers",
                "queue",
                "walltime",
            ]

    def __read_platform_config(self, conf_path: str):
        self.cp.read(conf_path)
        for section in self.cp.sections():
            for key in self.cp[section]:
                self.__config[f"{section}.{key}"] = self.cp[section][key]
        self.__config[Key.Platforms.platforms] = self.__parse_platform()

    def __read_experiment_config(self, conf_path: str):
        self.cp.read(conf_path)
        for section in self.cp.sections():
            for key in self.cp[section]:
                self.__config[f"{section}.{key}"] = self.cp[section][key]
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

    def __parse_platform(self):
        platforms = []
        for platform_name in self.cp[Key.Platforms.platforms].values():
            section = f"platforms.{platform_name.strip()}"
            platform = {key: self.cp[section][key] for key in self.cp[section]}
            platform["control"] = platform["control"].lower() == "true"
            platforms.append(platform)
        return platforms

    def __parse_load_generators(self):
        generators = []
        for generator_name in self.cp[Key.Experiment.Generators.generators].values():
            section = f"experiment.generators.{generator_name.strip()}"
            generator = {key: self.cp[section][key] for key in self.cp[section]}
            generators.append(generator)
        return generators

    def __parse_scaling_strategy(self):
        with open(self.get_str(Key.Experiment.Scaling.strategy_path), "r") as file:
            return yaml.safe_load(file)

    def get(self, key):
        return self.__config.get(key)

    def get_int(self, key):
        return int(self.get(key))

    def get_bool(self, key):
        return self.get_str(key).lower() == "true"

    def get_float(self, key):
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def get_list_str(self, key):
        return self.get_str(key).split(",")

    def get_list_int(self, key):
        return [int(value) for value in self.get_list_str(key)]

    def to_json(self):
        return json.dumps(self.__config, indent=4)

    def load_from_log(self, log_path: str):
        with open(log_path, "r") as f:
            lines = f.readlines()
        start_line = lines.index("[CONFIG]\n") + 1
        end_line = lines.index("[TIMESTAMPS]\n")
        self.__config = json.loads("".join(lines[start_line:end_line]))
