import os
import subprocess

import enoslib as en
import yaml

from scripts.src.Platform import Platform
from scripts.utils.Logger import Logger
from scripts.utils.Config import Config, Key


class G5k(Platform):
    def __init__(self, config: Config, log: Logger, verbose: bool = True):
        super().__init__()
        _ = en.init_logging()
        self.__log = log
        self.config = config

        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        if verbose:
            # Check that Grid5000 is joinable
            en.check()

        # Set up the reservation
        self.create(verbose)

    # Create a reservation
    def create(self, verbose: bool = True):
        self.reservation_name = self.config.get_str(Key.Platform.reservation_name)
        self.site = self.config.get_str(Key.Platform.site)
        self.cluster = self.config.get_str(Key.Platform.cluster)
        self.producers = self.config.get_int(Key.Platform.producers)
        self.consumers = self.config.get_int(Key.Platform.consumers)
        self.queue = self.config.get_str(Key.Platform.queue)
        self.walltime = self.config.get_str(Key.Platform.walltime)
        self.start_time = self.config.get(Key.Platform.start_time)

        # Setup request of resources as specified in configuration file
        network = en.G5kNetworkConf(type="prod", roles=["my_network"], site=self.site)

        # If producers or consumers are set to 0, crate a single node with 3 roles
        if self.producers == 0 or self.consumers == 0:
            conf = (
                en.G5kConf.from_settings(
                    job_name=self.reservation_name,
                    queue=self.queue,
                    walltime=self.walltime,
                )
                .add_network_conf(network)
                .add_machine(
                    roles=["control", "producers", "consumers"],
                    cluster=self.cluster,
                    nodes=1,
                    primary_network=network,
                )
                .finalize()
            )
        else:
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
