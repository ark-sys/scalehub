import enoslib as en
import os

from .Platform import Platform
from .utils import Logger
from .utils.Config import Config, Key


class G5k(Platform):
    def __init__(self, config: Config, log: Logger):
        super().__init__()
        _ = en.init_logging()
        self.__log = log

        # Create .python-grid5000.yaml required by enoslib
        self.create_credentials_file()

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
                job_type="allow_classic_ssh",
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

        # Setup NFS access
        self.enable_g5k_nfs_access()

    def enable_g5k_nfs_access(self):
        with open("/run/secrets/mysecretuser") as f:
            username = f.read()
        with open("/run/secrets/mysecretpass") as f:
            password = f.read()

        uri = f"https://api.grid5000.fr/3.0/sites/{self.job_site}/storage/home/{username}/access"

        import requests

        requests.post(
            uri,
            json={"termination": {"job": self.job_id, "site": self.job_site}},
            auth=(username, password),
        )
    def create_credentials_file(self):

        try:
            with open("/run/secrets/mysecretuser", "r") as user_file, open(
                "/run/secrets/mysecretpass", "r"
            ) as password_file:
                username = user_file.read().strip()
                password = password_file.read().strip()
            home_directory = os.path.expanduser("~")
            file_path = os.path.join(home_directory, ".python-grid5000.yaml")

            # Store credentials file
            with open(file_path, "w") as credentials_file:
                credentials_file.write(f"username: {username}\n")
                credentials_file.write(f"password: {password}\n")
            os.chmod(file_path, 0o600)
            self.__log.info("Credentials file created successfully.")

        except FileNotFoundError:
            self.__log.error("Error: Secrets files not found.")
        except Exception as e:
            self.__log.error(f"Error: {str(e)}")
    def get_platform_metadata(self) -> dict[str, str]:
        return dict(job_id=self.job_id)

    def destroy(self):
        # Destroy all resources from Grid5000
        self.provider.destroy()
