import json

import enoslib as en
import os

import yaml

from .Platform import Platform
from .utils import Logger
from .utils.Config import Config, Key


class G5k(Platform):
    def __init__(self, config: Config, log: Logger):
        super().__init__()
        self.password = None
        self.username = None
        _ = en.init_logging()
        self.__log = log

        # Create .python-grid5000.yaml required by enoslib
        self.check_credentials_file()

        # Check that Grid5000 is joinable
        en.check()

        self.name = config.get_str(Key.NAME)
        self.site = config.get_str(Key.SITE)
        self.cluster = config.get_str(Key.CLUSTER)
        self.controllers = config.get_int(Key.NUM_CONTROL)
        self.workers = config.get_int(Key.NUM_WORKERS)
        self.queue = config.get_str(Key.QUEUE_TYPE)
        self.walltime = config.get_str(Key.WALLTIME)

        # Setup request of resources as specified in configuration file
        network = en.G5kNetworkConf(type="prod", roles=["my_network"], site=self.site)
        conf = (
            en.G5kConf.from_settings(
                job_name=self.name,
                queue=self.queue,
                walltime=self.walltime,
            )
            .add_network_conf(network)
            .add_machine(
                roles=["control"],
                cluster=self.cluster,
                nodes=self.controllers,
                primary_network=network,
            )
            .add_machine(
                roles=["workers"],
                cluster=self.cluster,
                nodes=self.workers,
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
        self.post_setup()

        # Return inventory dictionary
        return inventory

    def post_setup(self):
        # Retrieve running job info
        jobs = self.provider.driver.get_jobs()
        self.job_id = jobs[0].uid
        self.job_site = jobs[0].site

        # Return home server and nfs share
        return self.retrieve_home_nfs()

    def retrieve_home_nfs(self):
        import requests

        try:
            uri = f"https://api.grid5000.fr/3.0/sites/{self.job_site}/storage/home/{self.username}/access"

            resp=requests.get(
                uri,
                auth=(self.username, self.password)
            )

            # Load the JSON content
            data = json.loads(resp.content)

            # Iterate through the dictionary keys to find 'nfs_address'
            for key, value in data.items():
                nfs_address = value.get('nfs_address')
                if nfs_address:
                    nfs_server, server_share = nfs_address.split(':', 1)
                    return nfs_server, server_share

            return None, None
        except(json.JSONDecodeError, AttributeError, ValueError) as e:
            self.__log.error(f"Error extracting NFS info: {e}")
            return None, None
        except requests.exceptions.RequestException as e:
            self.__log.error(f"Error requesting NFS info: {e}")

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
