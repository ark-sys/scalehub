from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader

from scripts.src.Platform import Platform
from scripts.utils.Logger import Logger


# TODO: Implement RaspberryPi class
class RaspberryPi(Platform):
    pico_inventory = "/app/conf/pico_hosts"

    def __init__(self, log: Logger, platform_config: dict):
        self.__log = log
        self.config = platform_config

    def setup(self) -> InventoryManager:
        dl = DataLoader()
        available_hosts = InventoryManager(loader=dl, sources=[self.pico_inventory])

        # Depending on the requested number of producers and consumers, create the inventory
        # file for the Raspberry Pi cluster
        producers = self.config["producers"]
        consumers = self.config["consumers"]
        control = self.config["control"]

        # Check if there are enough hosts available
        # Check if there are enough hosts available
        if len(available_hosts.get_hosts()) < producers + consumers + (
            1 if control else 0
        ):
            self.__log.error("Not enough hosts available")
            raise Exception("Not enough hosts available")

        # Create the inventory file
        inventory = InventoryManager(loader=dl, sources=[])
        inventory.add_group("producers")
        inventory.add_group("consumers")
        inventory.add_group("pico_hosts")
        inventory.add_group("all")

        # Add the hosts to the inventory file
        for host in available_hosts.get_hosts():
            if control:
                inventory.add_group("control")
                inventory.add_host(host.name, group="control")
                inventory.add_host(host.name, group="all")
                control = False
            if producers > 0:
                inventory.add_host(host.name, group="producers")
                inventory.add_host(host.name, group="all")
                producers -= 1
            elif consumers > 0:
                inventory.add_host(host.name, group="consumers")
                inventory.add_host(host.name, group="all")
                consumers -= 1
            inventory.add_host(host.name, group="pico_hosts")

        return inventory
