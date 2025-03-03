import subprocess

import yaml

from scripts.src.platforms.Platform import Platform
from scripts.utils.Logger import Logger


class RaspberryPiPlatform(Platform):
    def __init__(self, log: Logger, platform_config: dict):
        self.__log = log
        self.platform_config = platform_config

    def test_ssh(self, host: str) -> bool:
        self.__log.debuggg(f"[PI_PLT] Testing SSH connection to {host}")
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", host, "echo", "alive"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
            )
            self.__log.debuggg(f"[PI_PLT] SSH test result: {result}")
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False

    def setup(self) -> dict:
        self.__log.info("[PI_PLT] Setting up Raspberry Pi platform")
        # Load hosts from yaml file
        with open(self.platform_config["inventory"], "r") as file:
            hosts = yaml.safe_load(file)["pico"]["hosts"]

        required_producers = int(self.platform_config.get("producers", 0))
        required_consumers = int(self.platform_config.get("consumers", 0))

        inventory = {
            "producers": {"hosts": {}},
            "consumers": {"hosts": {}},
            "pico": {"hosts": {}},
            "agents": {"hosts": {}},
        }

        alive_hosts = [
            host for host in hosts if self.test_ssh(hosts[host]["ansible_ssh_host"])
        ]

        if len(alive_hosts) < required_producers + required_consumers:
            self.__log.error(
                "[PI_PLT] Not enough alive hosts to meet the requirements."
            )
            raise Exception("[PI_PLT] Not enough alive hosts to meet the requirements.")

        for i, host in enumerate(alive_hosts):
            if i < required_producers:
                inventory["pico"]["hosts"][host] = hosts[host]
                inventory["agents"]["hosts"][host] = hosts[host]
                inventory["producers"]["hosts"][host] = hosts[host]
                inventory["pico"]["hosts"][host]["cluster_role"] = "producer"
                self.__log.debugg(f"[PI_PLT] Adding producer {host}")
            elif i < required_producers + required_consumers:
                inventory["pico"]["hosts"][host] = hosts[host]
                inventory["agents"]["hosts"][host] = hosts[host]
                inventory["consumers"]["hosts"][host] = hosts[host]
                inventory["pico"]["hosts"][host]["cluster_role"] = "consumer"
                self.__log.debugg(f"[PI_PLT] Adding consumer {host}")

        self.__log.debugg(f"[PI_PLT] Final pi Inventory: {inventory}")
        return inventory

    def destroy(self):
        pass
