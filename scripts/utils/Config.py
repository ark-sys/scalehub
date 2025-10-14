# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import configparser as cp
import json
import os.path
from dataclasses import dataclass
from inspect import getmembers, isclass

import yaml

from scripts.utils.Defaults import DefaultKeys as Key, ConfigKey
from scripts.utils.Logger import Logger


@dataclass
class Config:
    RUNTIME_PATH = "/app/runtime/runtime.json"
    DEFAULTS_PATH = "/app/conf/defaults.ini"

    def __init__(self, log: Logger, _param: object):
        self.__config = {}
        self.__log = log
        if isinstance(_param, dict):
            self.__config = _param
        elif isinstance(_param, str):
            self.__load_from_file(_param)
        else:
            self.__log.error(f"Invalid type for conf: {type(_param)}. Expected path (str) or dict.")
            raise ValueError(f"Invalid type for conf: {type(_param)}")

    def __str__(self):
        return f"Config: {self.__config}"

    def __load_from_file(self, _param: str):
        if not os.path.exists(_param):
            self.__log.error(f"Configuration file {_param} not found or does not exist.")
            raise FileNotFoundError(f"Configuration file {_param} not found.")
        if _param.endswith(".ini"):
            self.__load_ini_file(_param)
        elif _param.endswith(".json"):
            self.__load_json_file(_param)
        else:
            self.__log.error(
                f"Invalid configuration file {_param}. Expected .ini file or log file."
            )
            raise ValueError(f"Invalid configuration file {_param}")

    def __load_json_file(self, _param: str):
        try:
            with open(_param, "r") as f:
                file_config = json.load(f)
                if "config" in file_config:
                    if isinstance(file_config["config"], str):
                        self.__config = json.loads(file_config["config"])
                    else:
                        self.__config = file_config["config"]
                else:
                    self.__config = file_config
        except Exception as e:
            self.__log.error(f"Error while loading json file: {str(e)}")
            raise e

    def __load_ini_file(self, _param: str):
        self.__init_defaults()
        if _param != self.DEFAULTS_PATH:
            parser = cp.ConfigParser()
            parser.read(_param)
            if parser.has_section("scalehub"):
                self.__validate_and_read_sections(parser, "scalehub", Key.Scalehub)
            if parser.has_section("experiment"):
                ignore_keys = ["steps", "generators"]
                self.__validate_and_read_sections(parser, "experiment", Key.Experiment, ignore_keys)
                if parser.has_section("experiment.scaling"):
                    self.__config[
                        Key.Experiment.Scaling.steps.key
                    ] = self.__parse_scaling_strategy()

                if parser.has_section("experiment.generators"):
                    self.__config[
                        Key.Experiment.Generators.generators.key
                    ] = self.__parse_load_generators(parser)

            if parser.has_section("platforms"):
                self.__validate_and_read_sections(parser, "platforms", Key.Platforms)

    def __init_defaults(self):
        parser = cp.ConfigParser()
        parser.read(self.DEFAULTS_PATH)
        for section in parser.sections():
            for key in parser[section]:
                dict_key = f"{section}.{key}"
                self.__config[dict_key] = parser[section][key]
        self.__config[Key.Experiment.Generators.generators.key] = self.__parse_load_generators(
            parser
        )

    def __validate_and_read_sections(self, parser, section_base, key_class, *args):
        ignored_keys = args[0] if args else []

        # Get class representations of the keys
        subclasses = [
            member
            for member in getmembers(key_class, isclass)
            if member[1].__module__ == key_class.__module__
        ]

        # Special handling for platforms section
        if section_base == "platforms":
            # Load keys from base section
            for key, value in parser.items(section_base):
                dict_key = f"{section_base}.{key}"
                self.__config[dict_key] = value

            # Load platform names
            platform_names = parser[section_base].get("platforms").split()
            self.__config[Key.Platforms.platforms.key] = platform_names
            platform_dicts = []
            for platform_name in platform_names:
                platform_name = platform_name.strip()
                section_name = f"{section_base}.{platform_name}"
                platform_dict = {"name": platform_name}
                if parser.has_section(section_name):
                    self.__config[f"{section_name}.name"] = platform_name
                    for key, value in parser.items(section_name):
                        dict_key = f"{section_name}.{key}"
                        self.__config[dict_key] = value
                        platform_dict[key] = value

                    # Check if platform has special firewall configuration
                    firewall_section = f"{section_name}.enos_firewall"
                    if parser.has_section(firewall_section):
                        firewall_dict = {}
                        for key, value in parser.items(firewall_section):
                            dict_key = f"{firewall_section}.{key}"
                            self.__config[dict_key] = value
                            firewall_dict[key] = value
                        platform_dict["enos_firewall"] = firewall_dict
                    platform_dicts.append(platform_dict)
                else:
                    raise ValueError(f"Section {section_name} not found in config file.")
            self.__config[Key.Platforms.platforms.key] = platform_dicts
        else:
            # Read base section parameters
            if parser.has_section(section_base):
                for key, value in parser.items(section_base):
                    dict_key = f"{section_base}.{key}"
                    if dict_key not in ignored_keys:
                        self.__config[dict_key] = value
            # Read subclass section parameters
            for subclass in subclasses:
                subclass_name = subclass[0].lower()
                section_name = f"{section_base}.{subclass_name}"
                if parser.has_section(section_name):
                    for key, value in parser.items(section_name):
                        dict_key = f"{section_name}.{key}"
                        if dict_key not in ignored_keys:
                            self.__config[dict_key] = value

        # Validate mandatory parameters
        self.__validate_mandatory_parameters(key_class)

    def __validate_mandatory_parameters(self, key_class):
        def __get_class_attributes(class_name, attr_type=ConfigKey) -> dict:
            return {
                pkey: pval for pkey, pval in vars(class_name).items() if isinstance(pval, attr_type)
            }

        # Get class representations of the keys
        subclasses = [
            member
            for member in getmembers(key_class, isclass)
            if member[1].__module__ == key_class.__module__
        ]

        # Check base class mandatory parameters
        for key, value in __get_class_attributes(key_class).items():
            if not value.is_optional and f"{key_class.__name__.lower()}.{key}" not in self.__config:
                raise ValueError(
                    f"Mandatory parameter {key} is missing in section {key_class.__name__.lower()}"
                )

        # Special handling for platforms section
        if key_class == Key.Platforms and Key.Platforms.platforms.key in self.__config:
            platforms = self.__config[Key.Platforms.platforms.key]

            for platform in platforms:
                for key, value in __get_class_attributes(Key.Platforms.Platform).items():
                    if not value.is_optional:
                        if value.kwargs.get("for_types"):
                            if platform["type"] not in value.kwargs.get("for_types"):
                                continue
                        if f"platforms.{platform['name']}.{key}" not in self.__config:
                            raise ValueError(
                                f"Mandatory parameter {key} is missing in section {key_class.__name__.lower()}.{platform['name']}"
                            )
        else:
            # Check subclass mandatory parameters
            for subclass in subclasses:
                for key, value in __get_class_attributes(subclass[1]).items():
                    if (
                        not value.is_optional
                        and f"{key_class.__name__.lower()}.{subclass[0].lower()}.{key}"
                        not in self.__config
                    ):
                        raise ValueError(
                            f"Mandatory parameter {key} is missing in section {key_class.__name__.lower()}.{subclass[0].lower()}"
                        )

    def get(self, key, default=None):
        return self.__config.get(key, default)

    def get_int(self, key, default=None) -> int:
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return int(value)

    def get_bool(self, key, default=None) -> bool:
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return str(value).lower() == "true"

    def get_float(self, key, default=None) -> float:
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return float(value)

    def get_str(self, key, default=None) -> str:
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return str(value)

    def get_list_str(self, key, default=None):
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return [str(v) for v in str(value).split()]

    def get_list_int(self, key, default=None):
        value = self.get(key, default)
        if value is None:
            raise ValueError(f"Configuration key '{key}' not found and no default provided")
        return [int(v) for v in str(value).split()]

    def update_runtime_file(self, create=False):
        try:
            if os.path.exists(self.RUNTIME_PATH):
                with open(self.RUNTIME_PATH, "r+") as f:
                    file_config = json.load(f)
                    file_config.update(self.__config)
                    f.seek(0)
                    f.truncate()
                    json.dump(file_config, f, indent=4)
            elif create:
                with open(self.RUNTIME_PATH, "w") as f:
                    json.dump(self.__config, f, indent=4)
            else:
                self.__log.error(
                    f"Runtime file {self.RUNTIME_PATH} does not exist. Create flag is set to False."
                )
        except Exception as e:
            self.__log.error(f"Error while updating runtime file: {str(e)}")
            raise e

    def delete_runtime_file(self):
        try:
            if os.path.exists(self.RUNTIME_PATH):
                os.remove(self.RUNTIME_PATH)
        except Exception as e:
            self.__log.error(f"Error while deleting runtime file: {str(e)}")
            raise e

    def __parse_load_generators(self, parser):
        load_generators_names = parser.get(
            Key.Experiment.Generators.generators.key, "generators"
        ).split()
        if not load_generators_names:
            self.__log.error("No load generators found in config file.")
            return None

        else:
            load_generators = []
            for name in load_generators_names:
                generator_section = f"{Key.Experiment.Generators.generators.key}.{name}"
                try:
                    generator = {
                        "name": name,
                        "type": parser[generator_section]["type"],
                        "topic": parser[generator_section]["topic"],
                        "num_sensors": int(parser[generator_section]["num_sensors"]),
                        "interval_ms": int(parser[generator_section]["interval_ms"]),
                        "replicas": int(parser[generator_section]["replicas"]),
                        "value": int(parser[generator_section]["value"]),
                    }
                    load_generators.append(generator)
                except KeyError as e:
                    self.__log.error(f"Error while parsing load generator {name}: {str(e)}")
                    raise e
            return load_generators

    def __parse_scaling_strategy(self):
        strategy_path = self.get_str(Key.Experiment.Scaling.strategy_path.key)
        try:
            with open(strategy_path, "r") as file:
                strategy = yaml.safe_load(file)
                return strategy
        except FileNotFoundError as e:
            self.__log.error(f"File not found: {strategy_path} - {str(e)}")
            raise e

    def to_json(self):
        try:
            return json.dumps(self.__config, indent=4)
        except Exception as e:
            self.__log.error(f"Error while converting config to json: {str(e)}")
            raise e
