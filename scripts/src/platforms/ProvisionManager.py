import os

import yaml

from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.EnosPlatforms import EnosPlatforms
from scripts.src.platforms.RaspberryPiPlatform import RaspberryPiPlatform
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class ProvisionManager:
    def __init__(self, log: Logger, config):
        self.__log = log
        self.__config = config

        self.inventory_dict = {}

        self.platforms = [
            self.__get_platforms(platform)
            for platform in self.__config.get(Key.Platforms.platforms.key)
            if self.__get_platforms(platform) is not None
        ]

        self.__log.debugg(f"====================")
        self.__log.debugg(f"[PROVISION_MGR] Platforms: {self.platforms}")

        self.enos_platforms = [
            platform
            for platform in self.platforms
            if isinstance(platform, EnosPlatform)
        ]

        self.__log.debugg(f"====================")
        self.__log.debugg(f"[PROVISION_MGR] Enos platforms: {self.enos_platforms}")

        self.raspberry_pis = [
            platform
            for platform in self.platforms
            if isinstance(platform, RaspberryPiPlatform)
        ]

        self.__log.debugg(f"====================")
        self.__log.debugg(
            f"[PROVISION_MGR] Raspberry Pi platforms: {self.raspberry_pis}"
        )

    def __get_platforms(self, platform_info):
        match platform_info["type"]:
            case "Grid5000" | "VMonG5k" | "FIT" | "VagrantG5k":
                return EnosPlatform(self.__log, platform_info)
            case "RaspberryPi":
                return RaspberryPiPlatform(self.__log, platform_info)
            case _:
                self.__log.error(
                    f"[PROVISION_MGR] Provision is not implemented for platform {platform_info['name']}, which is of type {platform_info['type']}"
                )
                return None

    def provision(self):
        self.__log.info("[PROVISION_MGR] Provisioning platforms")
        # Make sure that the inventory directory exists
        os.makedirs(self.__config.get(Key.Scalehub.inventory.key), exist_ok=True)

        if self.enos_platforms:
            self.__log.debug(
                "[PROVISION_MGR] Found Enos platforms. Generating inventory."
            )
            enos_providers = EnosPlatforms(self.__log, self.enos_platforms)
            enos_inventory = enos_providers.setup()

            inventory_path = os.path.join(
                self.__config.get(Key.Scalehub.inventory.key), "enos_inventory.yaml"
            )
            with open(inventory_path, "w") as inventory_file:
                yaml.dump(
                    enos_inventory,
                    inventory_file,
                    default_flow_style=False,
                )

            self.inventory_dict[inventory_path] = enos_inventory

            # Enable firewall
            if self.__config.get_bool(Key.Platforms.enable_ipv6.key):
                enos_providers.post_setup()
        # Retrieve pi nodes "statically"
        if self.raspberry_pis:
            self.__log.debug(
                "[PROVISION_MGR] Found Raspberry Pi platforms. Generating inventory."
            )
            pi_inventory = self.raspberry_pis[0].setup()

            inventory_path = os.path.join(
                self.__config.get(Key.Scalehub.inventory.key), "pi_inventory.yaml"
            )
            with open(inventory_path, "w") as inventory_file:
                yaml.dump(pi_inventory, inventory_file, default_flow_style=False)

            self.inventory_dict[inventory_path] = pi_inventory

        if not self.inventory_dict:
            self.__log.error(
                "[PROVISION_MGR] No platforms are specified in the configuration file."
            )
            raise Exception("[PROVISION_MGR] No platforms to provision.")
        else:
            self.__log.info("[PROVISION_MGR] Provisioning completed.")
            return self.inventory_dict

    def destroy(self):
        self.__log.info("[PROVISION_MGR] Destroying platforms")
        if self.enos_platforms:
            enos_providers = EnosPlatforms(self.__log, self.enos_platforms)
            enos_providers.destroy()
