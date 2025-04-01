import enoslib as en

from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.Platform import Platform
from scripts.utils.Logger import Logger


class EnosPlatforms(Platform):
    # contain multiple EnosPlatform objects
    # Advance reservations on enoslib are limited to 2 reservations at a time
    # the goal of this class is to merge enoslib configurations for the same platform type to reduce the number of reservations

    def __init__(self, log: Logger, platforms: [EnosPlatform]):
        self.__log = log
        self.platforms = platforms

        # If we have multiple enos platforms of the same type. let's merge their dicts
        enos_platforms_confs = self.__build_uber_dict(platforms)

        # Create providers
        self.enos_providers = self.__get_providers(enos_platforms_confs)

    # gen reservation name based on start_time
    def __gen_reservation_name(self, platform_type, start_time):
        reservation_name_base = "scalehub"

        platform_type = (
            "baremetal" if platform_type == "Grid5000" else "virtualmachines"
        )
        reservation_name = f"{reservation_name_base}_{platform_type}"

        if start_time != "now":
            time_parts = start_time.split(":")
            if len(time_parts) != 3:
                self.__log.error(
                    f"[ENOS_PLTS] Invalid start_time format for {platform_type}. Expected format: HH:MM:SS"
                )
            else:
                time_tag = "_late" if int(time_parts[0]) >= 19 else "_day"
                reservation_name = f"{reservation_name}{time_tag}"
                self.__log.debugg(f"[ENOS_PLTS] Reservation name: {reservation_name}")
                return reservation_name
        else:
            self.__log.debugg(f"[ENOS_PLTS] Reservation name: {reservation_name}")
            return reservation_name

    def __build_uber_dict(self, platforms) -> dict:
        platform_confs = {}
        for platform in platforms:
            if platform.platform_type not in platform_confs:
                # Create new entry in uber_conf for this platform type
                platform_confs[platform.platform_type] = platform.setup()
            else:
                # Merge confs
                conf = platform_confs[platform.platform_type]
                conf["resources"]["machines"] += platform.setup()["resources"][
                    "machines"
                ]
                platform_confs[platform.platform_type] = conf

        self.__log.debugg(
            f"[ENOS_PLTS] Merged platform configurations: {platform_confs}"
        )
        return platform_confs

    def __get_providers(self, conf_dict):
        providers = []
        self.__log.debugg(f"[ENOS_PLTS] Creating providers for {conf_dict}")

        for platform_type, conf in conf_dict.items():

            # Get provider using a platform of the same type
            for platform in self.platforms:
                if platform.platform_type == platform_type:
                    conf["job_name"] = self.__gen_reservation_name(
                        platform_type, platform.start_time
                    )
                    providers.append(platform.get_provider(platform_type, conf))
                    # break after finding the first platform of the same type. We may have more platforms than configs after a merge
                    break

        return providers

    # Update the inventory for vagrant group. Each host should host a certain amount of VMs based on the platform configuration
    def distribute_vagrant_vms(self, inventory: dict):
        vagrant_hypervisors = inventory["vagrant"]["hosts"]
        self.__log.debugg(f"[ENOS_PLTS] Vagrant hosts: {vagrant_hypervisors}")

        inventory["vms"] = {"hosts": {}}
        vagrant_platforms = [
            platform
            for platform in self.platforms
            if platform.platform_type == "VagrantG5k"
        ]
        self.__log.debugg(f"[ENOS_PLTS] Vagrant platforms: {vagrant_platforms}")

        for platform in vagrant_platforms:
            platform_vms = platform.vm_groups
            platform_name = platform.platform_config["name"]
            filtered_vagrant_hypervisors = [
                host
                for host in vagrant_hypervisors.values()
                if host["reservation_name"] == platform_name
            ]

            self.__log.debugg(
                f"[ENOS_PLTS] Platform {platform_name} VM groups: {platform_vms}"
            )
            required_nodes = sum(
                vm_group["required_nodes"] for vm_group in platform_vms
            )

            if len(filtered_vagrant_hypervisors) != required_nodes:
                self.__log.error(
                    f"[ENOS_PLTS] Number of hosts for platform {platform_name} does not match the required number of nodes for VM groups"
                )
                exit(1)

            for vm_group in platform_vms:
                vm_count = vm_group["count"]
                vm_required_nodes = vm_group["required_nodes"]

                if vm_count == 0:
                    self.__log.debug(
                        f"[ENOS_PLTS] VM group {vm_group} has a count of 0. Skipping."
                    )
                    continue

                self.__log.debugg(
                    f"[ENOS_PLTS] Distributing {vm_count} VMs for VM group {vm_group}"
                )
                candidate_hypervisors = [
                    filtered_vagrant_hypervisors.pop() for _ in range(vm_required_nodes)
                ]

                for i in range(vm_count):
                    vm_hypervisor = candidate_hypervisors[i % vm_required_nodes][
                        "ansible_host"
                    ]
                    vm_name = f"vm-{vm_hypervisor.split('.', 1)[0]}-{(i // vm_required_nodes) + 1}"
                    inventory["vms"]["hosts"][vm_name] = {
                        "ansible_host": vm_name,
                        "ansible_user": "root",
                        "ansible_connection": "ssh",
                        "hypervisor": vm_hypervisor,
                        "cluster_role": vm_group["role"][:-1],
                        "vm_type": "vagrant",
                        "core_per_vm": vm_group["conf"]["core_per_vm"],
                        "memory_per_vm": vm_group["conf"]["memory_per_vm"],
                        "disk_per_vm": vm_group["conf"]["disk_per_vm"],
                        "site": vm_group["conf"]["site"],
                        "cluster": vm_group["conf"]["cluster"],
                    }
                    inventory["vagrant"]["hosts"][vm_hypervisor]["vm_count"] = (
                        inventory["vagrant"]["hosts"][vm_hypervisor].get("vm_count", 0)
                        + 1
                    )
                    self.__log.debugg(
                        f"[ENOS_PLTS] Added VM {vm_name} to hypervisor {vm_hypervisor}"
                    )

        return inventory

    def __reformat_inventory(self, inventory) -> dict:
        self.__log.debug(f"[ENOS_PLTS] Initial enos Inventory: {inventory}")

        # Remove platform_name groups and add attribute
        for platform in self.platforms:
            platform_name = platform.platform_config["name"]
            if platform_name in inventory:
                for host in inventory[platform_name]["hosts"]:
                    for group in inventory:
                        if group != platform_name and host in inventory[group]["hosts"]:
                            inventory[group]["hosts"][host][
                                "reservation_name"
                            ] = platform_name
                inventory.pop(platform_name)

        # Distribute VMs for vagrant group
        if "vagrant" in inventory:
            inventory = self.distribute_vagrant_vms(inventory)

        # Refactor producers and consumers groups
        for role in ["producers", "consumers"]:
            if role in inventory:
                for host in inventory[role]["hosts"]:
                    for group in inventory:
                        if group != role and host in inventory[group]["hosts"]:
                            inventory[group]["hosts"][host]["cluster_role"] = role[:-1]
                inventory.pop(role)

        if "control" in inventory:
            for host in inventory["control"]["hosts"]:
                inventory["control"]["hosts"][host]["cluster_role"] = "control"

        # Create agents group
        inventory["agents"] = {"hosts": {}}
        if "vms" in inventory:
            for host in inventory["vms"]["hosts"]:
                if (
                    "control" not in inventory
                    or host not in inventory["control"]["hosts"]
                ):
                    inventory["agents"]["hosts"][host] = inventory["vms"]["hosts"][host]
            inventory.pop("vms")
        if "virtualmachine" in inventory:
            for host in inventory["virtualmachine"]["hosts"]:
                if (
                    "control" not in inventory
                    or host not in inventory["control"]["hosts"]
                ):
                    inventory["agents"]["hosts"][host] = inventory["virtualmachine"][
                        "hosts"
                    ][host]
            inventory.pop("virtualmachine")

        # Refactor baremetal group
        if "baremetal" in inventory:
            inventory.setdefault("G5k", {"hosts": {}})
            for host in inventory["baremetal"]["hosts"]:
                inventory["G5k"]["hosts"][host] = inventory["baremetal"]["hosts"][host]
                if (
                    "control" not in inventory
                    or host not in inventory["control"]["hosts"]
                ):
                    inventory["agents"]["hosts"][host] = inventory["baremetal"][
                        "hosts"
                    ][host]
            inventory.pop("baremetal")

        self.__log.debug(f"[ENOS_PLTS] Final enos Inventory: {inventory}")
        return inventory

    def setup(self) -> dict:
        # Request nodes with enoslib
        if self.enos_providers:
            self.__log.debug("[ENOS_PLTS] Found Enos platforms. Provisioning...")
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
            elif start_time == "now":
                start_time = None

            self.__log.debugg(f"[ENOS_PLTS] Start time: {start_time}")
            roles, networks = providers.init(start_time=start_time)

            # Fill in network information from nodes
            roles = en.sync_info(roles, networks)

            inventory = {}
            # Collect hosts for each role
            for group, hosts in roles.items():
                if group not in inventory:
                    inventory[group] = {"hosts": {}}
                for host in hosts:
                    if group == "VMonG5k":
                        if host.alias not in inventory[group]["hosts"]:
                            inventory[group]["hosts"][host.alias] = {
                                "ansible_host": host.address,
                                "hypervisor": host.pm.alias,
                                "ansible_user": "root",
                            }

                            # Add hypervisor to G5k group
                            if host.pm.alias not in inventory["G5k"]["hosts"]:
                                # ip6_alias = f"{host.pm.alias('.')[0]}-ipv6.{host.pm.alias('.', 1)[1]}"
                                inventory["G5k"]["hosts"][host.pm.alias] = {
                                    "ansible_host": host.pm.address,
                                    "ansible_user": "root",
                                    "cluster_role": "hypervisor",
                                    # "ipv6_alias": ip6_alias,
                                }

                    elif group == "G5k":
                        ip6_alias = f"{host.address.split('.')[0]}-ipv6.{host.address.split('.', 1)[1]}"
                        if host.alias not in inventory[group]["hosts"]:
                            inventory[group]["hosts"][host.alias] = {
                                "ansible_host": host.address,
                                "ansible_user": "root",
                                "ipv6_alias": ip6_alias,
                            }
                    else:
                        if host.alias not in inventory[group]["hosts"]:
                            inventory[group]["hosts"][host.alias] = {
                                "ansible_host": host.address,
                                "ansible_user": "root",
                            }

            self.__log.debug(f"[ENOS_PLTS] Roles: {roles}")
            self.__log.debug(f"[ENOS_PLTS] Networks: {networks}")

            inventory = self.__reformat_inventory(inventory)

            return inventory

    def post_setup(self):
        # Apply firewall rules
        for provider in self.enos_providers:
            if isinstance(provider, en.G5k):
                try:
                    # apply rules to G5k group from self.enos_inventory
                    # Retrieve firewall rules from platform_config
                    # TODO: Implement this
                    provider.fw_create(proto="all")
                except Exception as e:
                    self.__log.warning(
                        f"[ENOS_PLTS] Error while applying firewall rules for {provider}: {str(e)}"
                    )
                    continue

    def destroy(self):
        for provider in self.enos_providers:
            provider.destroy()
        self.__log.info("[ENOS_PLTS] Enos platforms destroyed.")
