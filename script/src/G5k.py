import json
import subprocess

import enoslib as en
import os

import yaml

from .Platform import Platform
from .utils import Logger
from .utils.Config import Config, Key


class G5k(Platform):
    def __init__(self, config: Config, log: Logger):
        super().__init__()
        _ = en.init_logging()
        self.__log = log
        self.config = config
        
        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        # Check that Grid5000 is joinable
        en.check()
        
        # Set up the reservation
        self.create()


    # Create a reservation
    def create(self):
        self.reservation_name = self.config.get_str(Key.Platform.reservation_name)
        self.site = self.config.get_str(Key.Platform.site)
        self.cluster = self.config.get_str(Key.Platform.cluster)
        self.producers = self.config.get_int(Key.Platform.producers)
        self.consumers = self.config.get_int(Key.Platform.consumers)
        self.queue = self.config.get_str(Key.Platform.queue)
        self.walltime = self.config.get_str(Key.Platform.walltime)

        # Setup request of resources as specified in configuration file
        network = en.G5kNetworkConf(type="prod", roles=["my_network"], site=self.site)
        conf = (
            en.G5kConf.from_settings(
                job_name=self.reservation_name,
                queue=self.queue,
                walltime=self.walltime,
            )
            .add_network_conf(network)
            .add_machine(
                roles=["control"],
                cluster=self.cluster,
                nodes=1,
                primary_network=network,
            )
            .add_machine(
                roles=["producers"],
                cluster=self.cluster,
                nodes=self.producers,
                primary_network=network,
            )
            .add_machine(
                roles=["consumers"],
                cluster=self.cluster,
                nodes=self.consumers,
                primary_network=network,
            )
            .finalize()
        )

        self.conf = conf
        self.provider = en.G5k(self.conf)
    def setup(self):
        # Request resources from Grid5000
        roles, networks = self.provider.init()
        inventory = ""

        for role, hosts in roles.items():
            # Add role header
            inventory += f"[{role}]\n"

            # Add hosts for the role
            for host in hosts:
                inventory += f"{host.alias}\n"

            # Add an extra newline to separate roles
            inventory += "\n"
        # Return inventory dictionary
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
        cmd = f"rsync -avz rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc/ {experiments_path}"

        self.__log.info(f"Syncing data from Grid5000 to {experiments_path}")
        # Execute the command
        subprocess.run(cmd, shell=True)