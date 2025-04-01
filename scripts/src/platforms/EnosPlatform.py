import json
import os
import subprocess

import enoslib as en
import yaml

from scripts.src.platforms.Platform import Platform
from scripts.utils.Logger import Logger


class EnosPlatform(Platform):
    # We distinguish producers and consumers from control instead of just workers
    # This is because in some cases (e.g. vms) we need hosts to be grouped in the same node
    base_roles = ["control", "producers", "consumers"]
    GRID5000_API = "https://api.grid5000.fr/stable"

    prod_network = {
        "networks": [{"id": "default", "type": "prod", "roles": ["my_network"]}]
    }

    def __init__(self, log: Logger, platform_config: dict):
        super().__init__()
        self.username = None
        self.password = None
        _ = en.init_logging()
        self.__log = log

        self.platform_config = platform_config

        self.__log.debuggg(f"[ENOS_PLT] Platform config: {self.platform_config}")

        self.start_time = platform_config["start_time"]

        self.base_conf = {
            "job_name": platform_config["reservation_name"],
            "walltime": platform_config["walltime"],
            "queue": platform_config["queue"],
        }

        self.platform_type = platform_config["type"]

    def __create_g5k_conf(self, platform_config, roles):
        provider_conf = self.base_conf.copy()
        provider_conf["resources"] = self.prod_network.copy()
        # Set network site
        provider_conf["resources"]["networks"][0]["site"] = platform_config["site"]

        # Add machines to provision
        provider_conf["resources"]["machines"] = []

        for role in roles:
            node_count = int(platform_config[role])
            if node_count > 0:
                extended_roles = [role, platform_config["name"]]
                if platform_config["type"] == "VagrantG5k":
                    extended_roles.append("vagrant")
                else:
                    extended_roles.append("baremetal")
                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": platform_config["cluster"],
                        "nodes": node_count,
                        "primary_network": "default",
                    }
                )

        return provider_conf

    def __create_vmong5k_conf(self, platform_config, roles):
        provider_conf = self.base_conf.copy()
        # Add network to provision
        provider_conf["resources"] = {"networks": []}
        # Add machines to provision
        provider_conf["resources"]["machines"] = []
        for role in roles:
            node_count = int(platform_config[role])
            if node_count > 0:
                extended_roles = [
                    role,
                    "virtualmachine",
                    platform_config["name"],
                ]
                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": platform_config["cluster"],
                        "number": node_count,
                        "vcore_type": "core",
                        "flavour_desc": {
                            "core": int(platform_config["core_per_vm"]),
                            "mem": int(platform_config["memory_per_vm"]),
                            "disk": int(platform_config["disk_per_vm"]),
                        },
                    }
                )
        return provider_conf

    def __create_fit_conf(self, platform_config, roles):
        provider_conf = self.base_conf.copy()
        provider_conf["resources"] = self.prod_network.copy()
        provider_conf["resources"]["networks"][0]["site"] = platform_config["site"]
        # Add machines to provision
        provider_conf["resources"]["machines"] = []
        for role in roles:
            node_count = int(platform_config[role])
            if node_count > 0:
                extended_roles = [
                    role,
                    "iot",
                    platform_config["name"],
                ]
                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": platform_config["cluster"],
                        "number": node_count,
                        "archi": platform_config["archi"],
                    }
                )
        return provider_conf

    def __estimate_required_nodes(self, p_vm_groups, site, cluster):
        # This type of platform simply relies on a classic g5k configuration to provision a baremetal machine that will host the VMs
        # We try to mimic the behavior of VMonG5k by estimating the number of nodes needed to host the VMs
        nodes_url = f"{self.GRID5000_API}/sites/{site}/clusters/{cluster}/nodes"
        try:

            self.__log.debuggg(
                f"Fetching data from {nodes_url}, vm_groups: {p_vm_groups}, site: {site}, cluster: {cluster}"
            )
            ssh_command = f"ssh {site}.g5k 'curl -s {nodes_url}'"
            response = subprocess.check_output(ssh_command, shell=True)

            nodes_dict = json.loads(response)
            # IMPORTANT: here we assume that all nodes have the same resources. For this reason, we extract the resources from the first node
            node = nodes_dict["items"][0]

            node_cpu = node["architecture"]["nb_cores"]
            node_memory = node["main_memory"]["ram_size"]

            for group_i in p_vm_groups:
                vm_conf = group_i["conf"]
                count = group_i["count"]

                total_vm_cpu = vm_conf["core_per_vm"] * count
                total_vm_memory = vm_conf["memory_per_vm"] * 1024 * 1024 * count

                required_nodes_cpu = (total_vm_cpu + node_cpu - 1) // node_cpu
                required_nodes_memory = (
                    total_vm_memory + node_memory - 1024
                ) // node_memory

                # Update dict with ney key
                group_i["required_nodes"] = max(
                    required_nodes_cpu, required_nodes_memory
                )
        except Exception as e:
            self.__log.error(
                f"[ENOS_PLT] Error while fetching data from {nodes_url}: {str(e)}"
            )
            exit(1)

    def __create_custom_vagrantong5k_conf(self, platform_config, roles):
        self.__log.debug(
            f"[ENOS_PLT] Creating custom Vagrant on Grid5000 configuration. Roles: {roles} Platform config: {platform_config}"
        )
        # Get the number of nodes needed to host the VMs
        vm_groups = []
        for role in roles:
            vm_groups.append(
                {
                    "role": role,
                    "conf": {
                        "name": platform_config["name"],
                        "site": platform_config["site"],
                        "cluster": platform_config["cluster"],
                        "core_per_vm": int(platform_config["core_per_vm"]),
                        "memory_per_vm": int(platform_config["memory_per_vm"]),
                        "disk_per_vm": int(platform_config["disk_per_vm"]),
                    },
                    "count": int(platform_config[role]),
                }
            )

        # Update the provider_conf with the number of nodes needed to host the VMs
        self.__estimate_required_nodes(
            vm_groups, platform_config["site"], platform_config["cluster"]
        )

        self.__log.debugg(f"[ENOS_PLT] VM groups: {vm_groups}")
        self.vm_groups = vm_groups
        # Create new platform config with the number of nodes needed to host the VMs
        new_platform_config = platform_config.copy()
        for group in vm_groups:
            new_platform_config[group["role"]] = group["required_nodes"]

        self.__log.debugg(f"[ENOS_PLT] New platform config: {new_platform_config}")
        # Now that we know how many nodes are needed, we can create the provider_conf with __create_g5k_conf
        provider_conf = self.__create_g5k_conf(new_platform_config, roles)

        return provider_conf

    def get_provider(self, platform_type, conf_dict):
        self.__log.debugg(f"[ENOS_PLT] Getting provider for: {platform_type}.")
        self.__log.debugg(f"[ENOS_PLT] Conf dict: {conf_dict}")
        match platform_type:
            case "Grid5000" | "VagrantG5k":
                conf = en.G5kConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.G5k(finalized_conf)
            case "VMonG5k":
                conf = en.VMonG5kConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.VMonG5k(finalized_conf)
            case "FIT":
                conf = en.IotlabConf.from_dictionary(conf_dict)
                finalized_conf = conf.finalize()
                return en.Iotlab(finalized_conf)
            case _:
                self.__log.error(
                    f"[ENOS_PLT] Couldn't get provider for: {platform_type}"
                )
                exit(1)

    def check_credentials_file(self):
        self.__log.debugg("[ENOS_PLT] Checking credentials file.")
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

    def __get_conf_dict(self, platform_type):
        self.__log.debugg(f"[ENOS_PLT] Creating Enos platform of type: {platform_type}")
        match platform_type:
            case "Grid5000":
                return self.__create_g5k_conf(self.platform_config, self.base_roles)
            case "VMonG5k":
                return self.__create_vmong5k_conf(self.platform_config, self.base_roles)
            case "FIT":
                return self.__create_fit_conf(self.platform_config, self.base_roles)
            case "VagrantG5k":
                return self.__create_custom_vagrantong5k_conf(
                    self.platform_config, self.base_roles
                )
            case _:
                self.__log.error(
                    f"[ENOS_PLT] Unsupported platform type: {self.platform_config['type']}"
                )
                exit(1)

    def setup(self, verbose: bool = False):
        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        if verbose:
            en.check()

        conf_dict = self.__get_conf_dict(self.platform_config["type"])

        # Simply return conf_dict to EnosPlatforms
        return conf_dict

    def destroy(self):
        platform_type = self.platform_config["type"]
        conf_dict = self.__get_conf_dict(platform_type)
        provider = self.get_provider(platform_type=platform_type, conf_dict=conf_dict)
        provider.destroy()
