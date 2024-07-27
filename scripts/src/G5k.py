import os
import subprocess

import enoslib as en
import yaml
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader

from scripts.src.Platform import Platform
from scripts.utils.Config import Key
from scripts.utils.Logger import Logger


class G5k(Platform):
    def __init__(self, log: Logger, platform_config: dict, verbose: bool = True):
        super().__init__()
        _ = en.init_logging()
        self.__log = log
        self.config = platform_config

        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        if verbose:
            # Check that Grid5000 is joinable
            en.check()

        # Set up the reservation
        self.create(verbose)

    # Create a reservation
    def create(self, verbose: bool = True):
        self.reservation_name = self.config["reservation_name"]
        self.site = self.config["site"]
        self.cluster = self.config["cluster"]
        self.producers = self.config["producers"]
        self.consumers = self.config["consumers"]
        self.queue = self.config["queue"]
        self.walltime = self.config["walltime"]
        self.start_time = self.config["start_time"]
        self.control = self.config["control"]

        # Setup request of resources as specified in configuration file
        network = en.G5kNetworkConf(type="prod", roles=["my_network"], site=self.site)

        # Create base configuration
        conf = en.G5kConf.from_settings(
            job_name=self.reservation_name,
            queue=self.queue,
            walltime=self.walltime,
        ).add_network_conf(network)

        # If control is not set, don't add it to the configuration
        if not self.control:
            # If both producers and consumers are 0, throw an error
            if self.producers == 0 and self.consumers == 0:
                self.__log.error(
                    "No control, producers or consumers specified in configuration file."
                )
                raise Exception(
                    "No control, producers or consumers specified in configuration file."
                )
            else:
                if self.producers > 0:
                    conf.add_machine(
                        roles=["producers"],
                        cluster=self.cluster,
                        nodes=self.producers,
                        primary_network=network,
                    )
                if self.consumers > 0:
                    conf.add_machine(
                        roles=["consumers"],
                        cluster=self.cluster,
                        nodes=self.consumers,
                        primary_network=network,
                    )
        else:
            conf.add_machine(
                roles=["control"],
                cluster=self.cluster,
                nodes=1,
                primary_network=network,
            )

            if self.producers > 0:
                conf.add_machine(
                    roles=["producers"],
                    cluster=self.cluster,
                    nodes=self.producers,
                    primary_network=network,
                )
            if self.consumers > 0:
                conf.add_machine(
                    roles=["consumers"],
                    cluster=self.cluster,
                    nodes=self.consumers,
                    primary_network=network,
                )

        self.conf = conf.finalize()
        self.provider = en.G5k(self.conf)

    def setup(self):

        # If start_time is set, convert it to an int timestamp. Format is "HH:MM:SS" of the day
        if self.start_time is not None:
            import datetime

            now = datetime.datetime.now()
            start_time = datetime.datetime.strptime(self.start_time, "%H:%M:%S")
            start_time = now.replace(
                hour=start_time.hour, minute=start_time.minute, second=start_time.second
            )
            self.start_time = int(start_time.timestamp())
            # Request resources from Grid5000
            roles, networks = self.provider.init(start_time=self.start_time)
        else:
            # Request resources from Grid5000
            roles, networks = self.provider.init()

        inventory: InventoryManager = InventoryManager(loader=DataLoader())

        inventory.add_group("grid5000")
        # Add the roles to the inventory
        for role in roles:
            inventory.add_group(role)
            for host in roles[role]:
                host_name = host.address
                inventory.add_host(host_name, group=role)
                inventory.add_host(host_name, group="grid5000")
                inventory.add_host(host_name, group="all")

        # Get list of hosts of group "all"
        all_hosts = inventory.get_hosts("all")
        self.__log.debug(f"all_hosts: {all_hosts}")

        # Setup IPv6 on reserved nodes
        # https://www.grid5000.fr/w/Reconfigurable_Firewall
        # https://www.grid5000.fr/w/IPv6#IPv6_communication_from_the_Internet_to_a_grid5000_node
        # https://discovery.gitlabpages.inria.fr/enoslib/jupyter/fit_and_g5k/01_networking.html#Setting-up-IPv6
        # https://discovery.gitlabpages.inria.fr/enoslib/tutorials/grid5000.html#reconfigurable-firewall-open-ports-to-the-external-world

        try:
            self.provider.fw_create(proto="all")
        except Exception as e:
            self.__log.error(f"Error creating firewall: {e}")
        # en.run("dhclient -6 br0", all_hosts)
        # en.run("apt update && apt install -y nginx", all_hosts)

        # Return inventory
        return inventory

    def check_credentials_file(self):
        # Get the home directory
        home_directory = os.path.expanduser("~")

        # Specify the path to the credentials file
        credentials_file_path = os.path.join(home_directory, ".python-grid5000.yaml")

        # Check if the file exists
        if os.path.exists(credentials_file_path):
            # Load the YAML content from the file
            with open(credentials_file_path, "r") as file:
                credentials = yaml.safe_load(file)

                # Check if 'username' and 'password' keys exist in the YAML content
                if "username" in credentials and "password" in credentials:
                    # Check if 'username' and 'password' values are not empty
                    if credentials["username"] and credentials["password"]:
                        self.username = credentials["username"]
                        self.password = credentials["password"]
                        return True  # Credentials are present and non-empty
                    else:
                        return False  # Either 'username' or 'password' is empty
                else:
                    return False  # Either 'username' or 'password' is missing
        else:
            return False  # File does not exist

    # Check if there is an active reservation
    def check_reservation(self):
        # Get the list of active reservations
        jobs = self.provider.driver.get_jobs()

        if not jobs:
            # No active reservation
            return False
        else:
            # There should be only one job, so get the first one
            job_id = jobs[0].uid
            # Query job information with Grid5000 REST API
            import requests

            query = requests.get(
                f"https://api.grid5000.fr/3.0/sites/{self.site}/jobs/{job_id}",
                auth=(self.username, self.password),
            )

            walltime: int = query.json()["walltime"]
            start_time: int = query.json()["started_at"]

            # Eval expected end time
            end_time = start_time + walltime

            # Eval current time
            import datetime

            now = datetime.datetime.now()

            # Eval remaining time
            remaining_time = end_time - now.timestamp()

            formatted_remaining_time = datetime.timedelta(seconds=remaining_time)

            return formatted_remaining_time

    # def extend_reservation(self, walltime):
    #     # Extend the reservation
    #     self.provider.driver.extend_job(self.job_id, walltime)
    # ssh rennes.g5k "oarwalltime \$(oarstat -u | tail -n 1 | cut -d ' ' -f 1) +2:00"
    def destroy(self):
        # Destroy all resources from Grid5000
        self.provider.destroy()

    def sync_data(self):
        experiments_path = self.config.get_str(Key.Scalehub.experiments)
        # rsync command from rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc to config.get_str(Key.Scalehub.experiments)
        cmd = f"rsync -avz --ignore-existing rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc/ {experiments_path}"

        self.__log.info(f"Syncing data from Grid5000 to {experiments_path}")
        # Execute the command
        subprocess.run(cmd, shell=True)


class G5k_VM(Platform):
    def __init__(self, log: Logger, platform_config: dict, verbose: bool = True):
        super().__init__()
        _ = en.init_logging()
        self.__log = log
        self.config = platform_config

        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        if verbose:
            # Check that Grid5000 is joinable
            en.check()

        # Performance tuning for VM interactions https://discovery.gitlabpages.inria.fr/enoslib/tutorials/performance_tuning.html#performance-tuning
        os.environ["ANSIBLE_PIPELINING"] = "True"

        # Set up the reservation
        self.create(verbose)

    # Create a reservation
    def create(self, verbose: bool = True):
        self.reservation_name = self.config["reservation_name"]
        self.site = self.config["site"]
        self.cluster = self.config["cluster"]
        self.producers = self.config["producers"]
        self.consumers = self.config["consumers"]
        self.queue = self.config["queue"]
        self.walltime = self.config["walltime"]
        self.start_time = self.config["start_time"]
        self.control = self.config["control"]
        self.core_per_vm = self.config["core_per_vm"]
        self.memory_per_vm = self.config["memory_per_vm"]
        self.disk_per_vm = self.config["disk_per_vm"]

        # Create base configuration
        conf = en.VMonG5kConf.from_settings(
            job_name=self.reservation_name,
            queue=self.queue,
            walltime=self.walltime,
        )

        # If control is not set, don't add it to the configuration
        if not self.control:
            # If both producers and consumers are 0, throw an error
            if self.producers == 0 and self.consumers == 0:
                self.__log.error(
                    "No control, producers or consumers specified in configuration file."
                )
                raise Exception(
                    "No control, producers or consumers specified in configuration file."
                )
            else:
                if self.producers > 0:
                    conf.add_machine(
                        roles=["producers"],
                        cluster=self.cluster,
                        number=self.producers,
                        vcore_type="core",
                        flavour_desc={
                            "core": self.core_per_vm,
                            "mem": self.memory_per_vm,
                            # "disk": self.disk_per_vm,
                        },
                    )
                if self.consumers > 0:
                    conf.add_machine(
                        roles=["consumers"],
                        cluster=self.cluster,
                        number=self.consumers,
                        vcore_type="core",
                        flavour_desc={
                            "core": self.core_per_vm,
                            "mem": self.memory_per_vm,
                            # "disk": self.disk_per_vm,
                        },
                    )
        else:
            conf.add_machine(
                roles=["control"],
                cluster=self.cluster,
                number=1,
                vcore_type="core",
                flavour_desc={
                    "core": self.core_per_vm,
                    "mem": self.memory_per_vm,
                    # "disk": self.disk_per_vm,
                },
            )

            if self.producers > 0:
                conf.add_machine(
                    roles=["producers"],
                    cluster=self.cluster,
                    number=self.producers,
                    vcore_type="core",
                    flavour_desc={
                        "core": self.core_per_vm,
                        "mem": self.memory_per_vm,
                        # "disk": self.disk_per_vm,
                    },
                )
            if self.consumers > 0:
                conf.add_machine(
                    roles=["consumers"],
                    cluster=self.cluster,
                    number=self.consumers,
                    vcore_type="core",
                    flavour_desc={
                        "core": self.core_per_vm,
                        "mem": self.memory_per_vm,
                        # "disk": self.disk_per_vm,
                    },
                )
        self.conf = conf.finalize()
        self.provider = en.VMonG5k(self.conf)

    def setup(self):

        # If start_time is set, convert it to an int timestamp. Format is "HH:MM:SS" of the day
        if self.start_time is not None:
            import datetime

            now = datetime.datetime.now()
            start_time = datetime.datetime.strptime(self.start_time, "%H:%M:%S")
            start_time = now.replace(
                hour=start_time.hour, minute=start_time.minute, second=start_time.second
            )
            self.start_time = int(start_time.timestamp())
            # Request resources from Grid5000
            roles, networks = self.provider.init(start_time=self.start_time)
        else:
            # Request resources from Grid5000
            roles, networks = self.provider.init()
        # Display the mapping from VM to physical nodes
        for role, vms in roles.items():
            print(f"\n=== {role} ===")
            for vm in vms:
                print(f"vm is {vm}")
                print(f"{vm.alias:20} {vm.address:15} {vm.pm.alias}")

        inventory: InventoryManager = InventoryManager(loader=DataLoader())

        inventory.add_group("vm_grid5000")
        # Add the roles to the inventory
        for role in roles:
            inventory.add_group(role)
            for host in roles[role]:
                host_info = f"{host.alias} ansible_ssh_host={host.address} grid_node={host.pm.alias} node_size={self.core_per_vm},{self.memory_per_vm}"

                inventory.add_host(host_info, group=role)
                inventory.add_host(host_info, group="vm_grid5000")
                inventory.add_host(host_info, group="all")

        # Return inventory
        return inventory

    def check_credentials_file(self):
        # Get the home directory
        home_directory = os.path.expanduser("~")

        # Specify the path to the credentials file
        credentials_file_path = os.path.join(home_directory, ".python-grid5000.yaml")

        # Check if the file exists
        if os.path.exists(credentials_file_path):
            # Load the YAML content from the file
            with open(credentials_file_path, "r") as file:
                credentials = yaml.safe_load(file)

                # Check if 'username' and 'password' keys exist in the YAML content
                if "username" in credentials and "password" in credentials:
                    # Check if 'username' and 'password' values are not empty
                    if credentials["username"] and credentials["password"]:
                        self.username = credentials["username"]
                        self.password = credentials["password"]
                        return True  # Credentials are present and non-empty
                    else:
                        return False  # Either 'username' or 'password' is empty
                else:
                    return False  # Either 'username' or 'password' is missing
        else:
            return False  # File does not exist

    def destroy(self):
        # Destroy all resources from Grid5000
        self.provider.destroy()
