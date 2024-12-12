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
                self.__create_g5k()
            case "VM_on_Grid5000":
                self.__create_g5k_vm()
            case "FIT":
                self.__create_fit()
            case _:
                self.__log.error(f"Unsupported platform type: {self.platform_type}")
                exit(1)

    def __create_g5k(self):
        network = en.G5kNetworkConf(type="prod", roles=["my_network"], site=self.site)
        conf = en.G5kConf.from_settings(
            job_name=self.reservation_name,
            queue=self.queue,
            walltime=self.walltime,
        ).add_network_conf(network)

        if self.control:
            conf.add_machine(roles=["control"], cluster=self.cluster, nodes=1)
        if self.producers > 0:
            conf.add_machine(
                roles=["producers"], cluster=self.cluster, nodes=self.producers
            )
        if self.consumers > 0:
            conf.add_machine(
                roles=["consumers"], cluster=self.cluster, nodes=self.consumers
            )

        self.conf = conf.finalize()
        self.provider = en.G5k(self.conf)

    def __create_g5k_vm(self):
        self.core_per_vm = self.config["core_per_vm"]
        self.memory_per_vm = self.config["memory_per_vm"]
        self.disk_per_vm = self.config["disk_per_vm"]

        conf = en.VMonG5kConf.from_settings(
            job_name=self.reservation_name,
            queue=self.queue,
            walltime=self.walltime,
        )

        if self.control:
            conf.add_machine(
                roles=["control"],
                cluster=self.cluster,
                number=1,
                vcore_type="core",
                flavour_desc={"core": self.core_per_vm, "mem": self.memory_per_vm},
            )
        if self.producers > 0:
            conf.add_machine(
                roles=["producers"],
                cluster=self.cluster,
                number=self.producers,
                vcore_type="core",
                flavour_desc={"core": self.core_per_vm, "mem": self.memory_per_vm},
            )
        if self.consumers > 0:
            conf.add_machine(
                roles=["consumers"],
                cluster=self.cluster,
                number=self.consumers,
                vcore_type="core",
                flavour_desc={"core": self.core_per_vm, "mem": self.memory_per_vm},
            )

        self.conf = conf.finalize()
        self.provider = en.VMonG5k(self.conf)

    def __create_fit(self):
        self.archi = self.config["archi"]

        conf = en.IotlabConf.from_settings(
            job_name=self.reservation_name,
            walltime=self.walltime,
        )

        if self.control:
            conf.add_machine(
                roles=["control"], archi=self.archi, number=1, site=self.site
            )
        if self.producers > 0:
            conf.add_machine(
                roles=["producers"],
                archi=self.archi,
                number=self.producers,
                site=self.site,
            )
        if self.consumers > 0:
            conf.add_machine(
                roles=["consumers"],
                archi=self.archi,
                number=self.consumers,
                site=self.site,
            )

        self.conf = conf.finalize()
        self.provider = en.Iotlab(self.conf)

    def post_setup(self):
        if self.platform_type == "Grid5000":
            self.provider.fw_create(proto="all")

    def get_provider(self):
        return self.provider

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
