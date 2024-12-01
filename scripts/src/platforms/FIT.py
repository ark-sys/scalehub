import enoslib as en
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader

from scripts.src.Platform import Platform
from scripts.utils.Logger import Logger


class FIT(Platform):
    def __init__(self, log: Logger, platfom_config: dict, verbose: bool = True):
        super().__init__()
        self.__log = log
        self.config = platfom_config

        if verbose:
            en.check()
        self.create(verbose)

    # Set up the reservation
    def create(self, verbose: bool = True):
        self.reservation_name = self.config["reservation_name"]
        self.site = self.config["site"]
        self.archi = self.config["archi"]
        self.producers = self.config["producers"]
        self.consumers = self.config["consumers"]
        self.walltime = self.config["walltime"]
        self.start_time = self.config["start_time"]
        self.control = self.config["control"]

        # Create the configuration
        conf = en.IotlabConf.from_settings(
            job_name=self.reservation_name,
            walltime=self.walltime,
        )

        # If control is not set, don't add it to the configuration
        if not self.control:
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
        else:
            conf.add_machine(
                roles=["control"],
                archi=self.archi,
                number=1,
                site=self.site,
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
        # Create the reservation
        self.conf = conf.finalize()
        self.provider = en.Iotlab(self.conf)

    def setup(self):
        # If start_time is set, convert it to an int timestamp
        if self.start_time is not None:
            import datetime

            now = datetime.datetime.now()
            start_time = datetime.datetime.strptime(self.start_time, "%H:%M:%S")
            start_time = now.replace(
                hour=start_time.hour, minute=start_time.minute, second=start_time.second
            )
            self.start_time = int(start_time.timestamp())
            roles, networks = self.provider.init(start_time=self.start_time)

        else:
            roles, networks = self.provider.init()

        roles = en.sync_info(roles, networks)

        inventory: InventoryManager = InventoryManager(loader=DataLoader())

        inventory.add_group("fit")

        # Add the hosts to the inventory file
        for role in roles:
            inventory.add_group(role)
            for host in roles[role]:
                host_name = host.address
                inventory.add_host(host_name, group=role)
                inventory.add_host(host_name, group="fit")
                inventory.add_host(host_name, group="all")

        return inventory

    def destroy(self):
        self.provider.destroy()
        self.__log.info("Resources have been released.")
