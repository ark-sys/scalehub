import enoslib as en
from .Platform import Platform
from .utils.Config import Config, Key
from .utils.Misc import Misc

class G5k(Platform):
    def __init__(self,config: Config):
        super().__init__()
        _ = en.init_logging()

        # Create .python-grid5000.yaml required by enoslib
        Misc.create_credentials_file()

        en.check()

        self.name = config.get_str(Key.NAME)
        self.site = config.get_str(Key.SITE)
        self.cluster = config.get_str(Key.CLUSTER)
        self.controllers = config.get_int(Key.NUM_CONTROL)
        self.workers = config.get_int(Key.NUM_WORKERS)
        self.queue = config.get_str(Key.QUEUE_TYPE)
        self.walltime = config.get_str(Key.WALLTIME)

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
                roles=["worker"],
                cluster=self.cluster,
                nodes=self.workers,
                primary_network=network
            )
            .finalize()
        )
        self.conf = conf
        self.provider = en.G5k(self.conf)

    def setup(self):
        # Request resources from Grid5000
        roles, networks = self.provider.init()

        # Initialize dictionary store for inventory
        Inventory = {
            "all": {
                "children": {}}}
        for grp, hostset in roles.items():
            Inventory["all"]["children"][grp] = {}
            Inventory["all"]["children"][grp]["hosts"] = {}
            for host in hostset:
                Inventory["all"]["children"][grp]["hosts"][host.alias] = None
        self.post_setup()

        # Return inventory dictionary
        return Inventory

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
            json={
                "termination": {
                    "job": self.job_id,
                    "site": self.job_site}},
            auth=(username, password),
        )

    def get_platform_metadata(self) -> dict[str, str]:
        return dict(job_id=self.job_id)

    def teardown(self):
        # Destroy all resources from Grid5000
        self.provider.destroy()