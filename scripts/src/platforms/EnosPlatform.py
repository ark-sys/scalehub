import os

import enoslib as en
import yaml

from scripts.src.platforms.Platform import Platform
from scripts.utils.Logger import Logger


class EnosPlatform(Platform):
    def __init__(self, log: Logger, platform_config: dict, verbose: bool = True):
        super().__init__()
        _ = en.init_logging()
        self.__log = log
        self.config = platform_config
        self.platform_type = platform_config["type"]
        self.reservation_name = self.config["reservation_name"]
        self.site = self.config["site"]
        self.cluster = self.config.get("cluster")
        self.producers = self.config["producers"]
        self.consumers = self.config["consumers"]
        self.queue = self.config["queue"]
        self.walltime = self.config["walltime"]
        self.start_time = self.config.get("start_time")
        self.control = self.config["control"]

        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        if verbose:
            en.check()

        match self.platform_type:
            case "Grid5000":
                self.conf_dict = self.__create_g5k_conf()

            case "VM_on_Grid5000":
                self.conf_dict = self.__create_g5k_vm_conf()
            case "FIT":
                self.conf_dict = self.__create_fit_conf()
            case _:
                self.__log.error(f"Unsupported platform type: {self.platform_type}")
                exit(1)

        self.provider = self.get_provider(self.conf_dict)

    def __create_g5k_conf(self):

        provider_conf = {
            "job_name": self.reservation_name,
            "walltime": self.walltime,
            "queue": self.queue,
            "resources": {
                "machines": [
                    {
                        "roles": ["control"],
                        "cluster": self.cluster,
                        "nodes": 1,
                        "primary_network": "default",
                    }
                    if self.control
                    else {},
                    {
                        "roles": ["producers"],
                        "cluster": self.cluster,
                        "nodes": self.producers,
                        "primary_network": "default",
                    }
                    if self.producers and self.producers > 0
                    else {},
                    {
                        "roles": ["consumers"],
                        "cluster": self.cluster,
                        "nodes": self.consumers,
                        "primary_network": "default",
                    }
                    if self.consumers and self.consumers > 0
                    else {},
                ],
                "networks": [
                    {
                        "id": "default",
                        "type": "prod",
                        "roles": ["my_network"],
                        "site": self.site,
                    },
                ],
            },
        }

        # Remove empty dictionaries from the list
        provider_conf["resources"]["machines"] = [
            machine for machine in provider_conf["resources"]["machines"] if machine
        ]

        return provider_conf

    def __create_g5k_vm_conf(self):

        provider_conf = {
            "job_name": self.reservation_name,
            "walltime": self.walltime,
            "queue": self.queue,
            "resources": {
                "machines": [
                    {
                        "roles": ["control"],
                        "cluster": self.cluster,
                        "number": 1,
                        "vcore_type": "core",
                        "flavour_desc": {
                            "core": self.config["core_per_vm"],
                            "mem": self.config["memory_per_vm"],
                        },
                    }
                    if self.control
                    else {},
                    {
                        "roles": ["producers"],
                        "cluster": self.cluster,
                        "number": self.producers,
                        "vcore_type": "core",
                        "flavour_desc": {
                            "core": self.config["core_per_vm"],
                            "mem": self.config["memory_per_vm"],
                        },
                    }
                    if self.producers and self.producers > 0
                    else {},
                    {
                        "roles": ["consumers"],
                        "cluster": self.cluster,
                        "number": self.consumers,
                        "vcore_type": "core",
                        "flavour_desc": {
                            "core": self.config["core_per_vm"],
                            "mem": self.config["memory_per_vm"],
                        },
                    }
                    if self.consumers and self.consumers > 0
                    else {},
                ],
                "networks": [],
            },
        }

        # Remove empty dictionaries from the list
        provider_conf["resources"]["machines"] = [
            machine for machine in provider_conf["resources"]["machines"] if machine
        ]

        return provider_conf

    def __create_fit_conf(self):

        provider_conf = {
            "job_name": self.reservation_name,
            "walltime": self.walltime,
            "queue": self.queue,
            "resources": {
                "machines": [
                    {
                        "roles": ["control"],
                        "archi": self.config["archi"],
                        "number": 1,
                        "site": self.site,
                    }
                    if self.control
                    else {},
                    {
                        "roles": ["producers"],
                        "archi": self.config["archi"],
                        "number": self.producers,
                        "site": self.site,
                    }
                    if self.producers and self.producers > 0
                    else {},
                    {
                        "roles": ["consumers"],
                        "archi": self.config["archi"],
                        "number": self.consumers,
                        "site": self.site,
                    }
                    if self.consumers and self.consumers > 0
                    else {},
                ],
                "networks": [
                    {
                        "id": "default",
                        "type": "prod",
                        "roles": ["my_network"],
                        "site": self.site,
                    },
                ],
            },
        }

        # Remove empty dictionaries from the list
        provider_conf["resources"]["machines"] = [
            machine for machine in provider_conf["resources"]["machines"] if machine
        ]

        return provider_conf

    def get_provider(self, conf_dict):
        match self.platform_type:
            case "Grid5000":
                conf = en.G5kConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.G5k(finalized_conf)
            case "VM_on_Grid5000":
                conf = en.VMonG5kConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.VMonG5k(finalized_conf)
            case "FIT":
                conf = en.IotlabConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.Iotlab(finalized_conf)
            case _:
                self.__log.error(f"Unsupported platform type: {self.platform_type}")
                exit(1)

    def post_setup(self):
        if self.platform_type == "Grid5000":
            try:
                self.provider.fw_create(proto="all")
            except Exception as e:
                self.__log.warning(f"Error while creating firewall rules: {str(e)}")

    def check_credentials_file(self):
        home_directory = os.path.expanduser("~")
        credentials_file_path = os.path.join(home_directory, ".python-grid5000.yaml")
        if os.path.exists(credentials_file_path):
            with open(credentials_file_path, "r") as file:
                credentials = yaml.safe_load(file)
                if "username" in credentials and "password" in credentials:
                    if credentials["username"] and credentials["password"]:
                        self.username = credentials["username"]
                        self.password = credentials["password"]
                        return True
                    else:
                        return False
                else:
                    return False
        else:
            return False

    def destroy(self):
        self.provider.destroy()
