from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader

from scripts.src.Platform import Platform
from scripts.utils.Logger import Logger


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
        inventory.add_group("pico")
        inventory.add_group("all")

        host_iter = iter(available_hosts.get_hosts())

        # Add the hosts to the inventory file
        if control:
            inventory.add_group("control")
            control_host = next(host_iter)
            inventory.add_host(control_host.name, group="control")
            inventory.add_host(control_host.name, group="all")
            inventory.add_host(control_host.name, group="pico")

        for i in range(producers):
            producer_host = next(host_iter)
            inventory.add_host(producer_host.name, group="producers")
            inventory.add_host(producer_host.name, group="all")
            inventory.add_host(producer_host.name, group="pico")

        for i in range(consumers):
            consumer_host = next(host_iter)
            inventory.add_host(consumer_host.name, group="consumers")
            inventory.add_host(consumer_host.name, group="all")
            inventory.add_host(consumer_host.name, group="pico")

        return inventory

    def destroy(self):
        pass
