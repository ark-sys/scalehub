import configparser
import os

import enoslib as en
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader

from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.Platform import merge_inventories
from scripts.src.platforms.RaspberryPi import RaspberryPi
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class ProvisionManager:
    def __init__(self, log: Logger, config, platforms):
        self.enos_inventory = None
        self.pi_inventory = None
        self.__log = log
        self.__config = config
        self.platforms = [
            self.__get_platforms(platform)
            for platform in platforms
            if self.__get_platforms(platform) is not None
        ]

        self.enos_platforms = [
            platform
            for platform in self.platforms
            if isinstance(platform, EnosPlatform)
        ]

        # If we have multiple enos platforms of the same type. let's merge their dicts
        enos_platforms_confs = {}
        for platform in self.enos_platforms:
            if platform.platform_type not in enos_platforms_confs:
                enos_platforms_confs[platform.platform_type] = platform.conf_dict
            else:
                conf = enos_platforms_confs[platform.platform_type]

                conf["resources"]["machines"] += platform.conf_dict["resources"][
                    "machines"
                ]

                enos_platforms_confs[platform.platform_type] = conf

        # Create provider
        self.enos_providers = []

        # TODO Think of a better way to do this
        for platform_type in enos_platforms_confs:
            # Here we take the first platform available in enos_platforms to generate a provider with the newly compacted config
            for platform in self.enos_platforms:
                if platform.platform_type == platform_type:
                    reservation_time = platform.start_time
                    time_tag = ""
                    if reservation_time != "now":
                        # Get hour format and check if after 19:00
                        if len(reservation_time.split(":")) != 3:
                            self.__log.error(
                                f"Invalid start_time format for {platform_type}. Expected format: HH:MM:SS"
                            )
                            pass
                        else:
                            if int(reservation_time.split(":")[0]) >= 19:
                                time_tag = "_late"
                            else:
                                time_tag = "_day"
                    reservation_name = (
                        "baremetal"
                        if platform_type == "Grid5000"
                        else "virtualmachines"
                    )

                    enos_platforms_confs[platform_type][
                        "job_name"
                    ] = f"scalehub_{reservation_name}{time_tag if time_tag else ''}"
                    self.enos_providers.append(
                        platform.get_provider(
                            enos_platforms_confs[platform.platform_type]
                        )
                    )
                    break

        self.raspberry_pis = [
            platform for platform in self.platforms if isinstance(platform, RaspberryPi)
        ]

    def __get_platforms(self, platform_info):
        match platform_info["type"]:
            case "Grid5000" | "VM_on_Grid5000" | "FIT":
                return EnosPlatform(self.__log, platform_info, verbose=False)
            case "RaspberryPi":
                return RaspberryPi(self.__log, platform_info)
            case _:
                self.__log.error(
                    f"Provision is not implemented for platform {platform_info['name']}, which is of type {platform_info['type']}"
                )
                return None

    def __generate_enos_inventory(self, roles):
        inventory = InventoryManager(loader=DataLoader())
        groups = [
            "ungrouped",
            "all",
            "control",
            "producers",
            "consumers",
            "G5k",
            "VMonG5k",
        ]
        for group in groups:
            inventory.add_group(group)

        for role in roles:
            for host in roles[role]:
                if "virtual" in host.alias:
                    host_info = f"{host.alias} ansible_ssh_host={host.address} grid_node={host.pm.alias} ansible_ssh_user=root"
                    inventory.add_host(host_info, group="VMonG5k")
                elif "grid5000" in host.address:
                    ipv6_alias = f"{host.address.split('.')[0]}-ipv6.{host.address.split('.', 1)[1]}"
                    host_info = f"{host.address} ipv6_alias={ipv6_alias}"
                    inventory.add_host(host_info, group="G5k")
                else:
                    host_info = host.alias
                inventory.add_host(host_info, group="all")
                inventory.add_host(host_info, group=role)

        return inventory

    def provision(self):
        self.__log.info("Provisioning platforms")

        # Request nodes with enoslib
        if self.enos_providers:
            self.__log.info("Found Enos platforms. Provisioning...")
            providers = en.Providers(self.enos_providers)

            # Check if start_time is set on any of the platforms, if multiple platforms have start_time set, select the earliest one
            start_time = min(
                (
                    platform.start_time
                    for platform in self.platforms
                    if isinstance(platform, EnosPlatform) and platform.start_time
                ),
                default=None,
            )

            import datetime

            if start_time and start_time != "now":
                now = datetime.datetime.now()
                start_time = datetime.datetime.strptime(start_time, "%H:%M:%S")
                start_time = now.replace(
                    hour=start_time.hour,
                    minute=start_time.minute,
                    second=start_time.second,
                )
                start_time = int(start_time.timestamp())
            else:
                start_time = None
            roles, networks = providers.init(start_time=start_time)
            self.enos_inventory = self.__generate_enos_inventory(roles)
            for provider in self.enos_providers:
                try:
                    provider.fw_create(proto="all")
                except Exception as e:
                    self.__log.warning(
                        f"Error while creating firewall rules for {provider}: {str(e)}"
                    )
                    continue
        # Retrieve pi node "statically"
        if self.raspberry_pis:
            self.__log.info("Found Raspberry Pi platforms. Provisioning...")
            self.pi_inventory = self.raspberry_pis[0].setup()

        if self.enos_inventory and self.pi_inventory:
            inventory = merge_inventories(self.enos_inventory, self.pi_inventory)
        else:
            inventory = self.enos_inventory or self.pi_inventory

        if not inventory:
            self.__log.error("No platforms are specified in the configuration file.")
            raise Exception("No platforms to provision.")

        # Create ConfigParser object
        config_parser = configparser.ConfigParser(allow_no_value=True)

        # Add inventory to ConfigParser object
        for group in inventory.get_groups_dict():
            config_parser.add_section(group)
            for host in inventory.get_hosts(group):
                config_parser.set(group, host.name)

        self.__log.info("Platforms provisioned")

        # Save final inventory to a file
        inventory_path = os.path.join(self.__config.get_str(Key.Scalehub.inventory))

        with open(inventory_path, "w") as file:
            config_parser.write(file)

        self.__log.info(f"Inventory file written to {inventory_path}")

    def destroy(self):
        self.__log.info("Destroying platforms")
        if self.enos_providers:
            providers = en.Providers(self.enos_providers)
            providers.destroy()
