# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import enoslib as en
import yaml

from src.scalehub.platforms.Platform import Platform
from src.utils.Logger import Logger


@dataclass
class VMGroup:
    """Configuration for a VM group."""

    role: str
    conf: Dict[str, Any]
    count: int
    required_nodes: int = 0


class EnosConfigurationError(Exception):
    """Raised when Enos configuration is invalid."""

    pass


class EnosPlatform(Platform):
    """Platform implementation for Enos-based platforms."""

    BASE_ROLES = ["control", "producers", "consumers"]
    GRID5000_API = "https://api.grid5000.fr/stable"

    PROD_NETWORK = {"networks": [{"id": "default", "type": "prod", "roles": ["my_network"]}]}

    def __init__(self, log: Logger, platform_config: Dict[str, Any]):
        super().__init__(log, platform_config)
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._vm_groups: List[VMGroup] = []

        _ = en.init_logging()
        self._base_conf = self._create_base_config()

    def _validate_config(self) -> None:
        """Validate platform configuration."""
        required_fields = ["type", "reservation_name", "walltime", "queue"]
        for field in required_fields:
            if field not in self._platform_config:
                raise EnosConfigurationError(f"Missing required field: {field}")

    def _create_base_config(self) -> Dict[str, Any]:
        """Create base configuration for the platform."""
        return {
            "job_name": self._platform_config["reservation_name"],
            "walltime": self._platform_config["walltime"],
            "queue": self._platform_config["queue"],
        }

    def _create_g5k_config(self, roles: List[str]) -> Dict[str, Any]:
        """Create Grid5000 configuration."""
        provider_conf = self._base_conf.copy()
        provider_conf["resources"] = self.PROD_NETWORK.copy()
        provider_conf["resources"]["networks"][0]["site"] = self._platform_config["site"]
        provider_conf["resources"]["machines"] = []

        for role in roles:
            node_count = int(self._platform_config.get(role, 0))
            if node_count > 0:
                extended_roles = [role, self._platform_config["name"]]
                platform_type = "vagrant" if self.platform_type == "VagrantG5k" else "baremetal"
                extended_roles.append(platform_type)

                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": self._platform_config["cluster"],
                        "nodes": node_count,
                        "primary_network": "default",
                    }
                )

        return provider_conf

    def _create_vmong5k_config(self, roles: List[str]) -> Dict[str, Any]:
        """Create VMonG5k configuration."""
        provider_conf = self._base_conf.copy()
        provider_conf["resources"] = {"networks": [], "machines": []}

        for role in roles:
            node_count = int(self._platform_config.get(role, 0))
            if node_count > 0:
                extended_roles = [role, "virtualmachine", self._platform_config["name"]]
                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": self._platform_config["cluster"],
                        "number": node_count,
                        "vcore_type": "core",
                        "flavour_desc": {
                            "core": int(self._platform_config["core_per_vm"]),
                            "mem": int(self._platform_config["memory_per_vm"]),
                            "disk": int(self._platform_config["disk_per_vm"]),
                        },
                    }
                )
        return provider_conf

    def _create_fit_config(self, roles: List[str]) -> Dict[str, Any]:
        """Create FIT configuration."""
        provider_conf = self._base_conf.copy()
        provider_conf["resources"] = self.PROD_NETWORK.copy()
        provider_conf["resources"]["networks"][0]["site"] = self._platform_config["site"]
        provider_conf["resources"]["machines"] = []

        for role in roles:
            node_count = int(self._platform_config.get(role, 0))
            if node_count > 0:
                extended_roles = [role, "iot", self._platform_config["name"]]
                provider_conf["resources"]["machines"].append(
                    {
                        "roles": extended_roles,
                        "cluster": self._platform_config["cluster"],
                        "number": node_count,
                        "archi": self._platform_config["archi"],
                    }
                )
        return provider_conf

    def _estimate_required_nodes(self, vm_groups: List[VMGroup], site: str, cluster: str) -> None:
        """Estimate required nodes for VM groups."""
        nodes_url = f"{self.GRID5000_API}/sites/{site}/clusters/{cluster}/nodes"

        try:
            self._log.debuggg(f"Fetching data from {nodes_url}")
            ssh_command = f"ssh {site}.g5k 'curl -s {nodes_url}'"
            response = subprocess.check_output(ssh_command, shell=True)
            nodes_dict = json.loads(response)

            # Assume all nodes have the same resources
            node = nodes_dict["items"][0]
            node_cpu = node["architecture"]["nb_cores"]
            node_memory_kb = node["main_memory"]["ram_size"]
            node_memory_mb = node_memory_kb // 1024  # Convert kB to MB

            for vm_group in vm_groups:
                vm_conf = vm_group.conf
                count = vm_group.count

                total_vm_cpu = vm_conf["core_per_vm"] * count
                total_vm_memory_mb = vm_conf["memory_per_vm"] * count  # Already in MB

                required_nodes_cpu = (total_vm_cpu + node_cpu - 1) // node_cpu
                required_nodes_memory = (total_vm_memory_mb + node_memory_mb - 1) // node_memory_mb

                vm_group.required_nodes = max(required_nodes_cpu, required_nodes_memory)

        except Exception as e:
            raise EnosConfigurationError(f"Error fetching node data from {nodes_url}: {str(e)}")

    def _create_custom_vagrantong5k_config(self, roles: List[str]) -> Dict[str, Any]:
        """Create custom Vagrant on Grid5000 configuration."""
        self._log.debug("Creating custom Vagrant on Grid5000 configuration")

        vm_groups = []
        for role in roles:
            vm_groups.append(
                VMGroup(
                    role=role,
                    conf={
                        "name": self._platform_config["name"],
                        "site": self._platform_config["site"],
                        "cluster": self._platform_config["cluster"],
                        "core_per_vm": int(self._platform_config["core_per_vm"]),
                        "memory_per_vm": int(self._platform_config["memory_per_vm"]),
                        "disk_per_vm": int(self._platform_config["disk_per_vm"]),
                    },
                    count=int(self._platform_config.get(role, 0)),
                )
            )

        self._estimate_required_nodes(
            vm_groups, self._platform_config["site"], self._platform_config["cluster"]
        )
        self._vm_groups = vm_groups

        # Create new platform config with required nodes
        new_platform_config = self._platform_config.copy()
        for vm_group in vm_groups:
            new_platform_config[vm_group.role] = vm_group.required_nodes

        return self._create_g5k_config(roles)

    def get_provider(self, platform_type: str, conf_dict: Dict[str, Any]):
        """Get the appropriate provider based on platform type."""
        self._log.debugg(f"Getting provider for: {platform_type}")

        provider_map = {
            "Grid5000": lambda: en.G5k(en.G5kConf.from_dictionary(conf_dict).finalize()),
            "VagrantG5k": lambda: en.G5k(en.G5kConf.from_dictionary(conf_dict).finalize()),
            "VMonG5k": lambda: en.VMonG5k(en.VMonG5kConf.from_dictionary(conf_dict).finalize()),
            "FIT": lambda: en.Iotlab(en.IotlabConf.from_dictionary(conf_dict).finalize()),
        }

        provider_factory = provider_map.get(platform_type)
        if not provider_factory:
            raise EnosConfigurationError(f"Unsupported platform type: {platform_type}")

        return provider_factory()

    def check_credentials_file(self) -> bool:
        """Check if credentials file exists and is valid."""
        self._log.debugg("Checking credentials file")
        home_directory = os.path.expanduser("~")
        credentials_file_path = os.path.join(home_directory, ".python-grid5000.yaml")

        if not os.path.exists(credentials_file_path):
            return False

        try:
            with open(credentials_file_path, "r") as file:
                credentials = yaml.safe_load(file)
                if credentials.get("username") and credentials.get("password"):
                    self._username = credentials["username"]
                    self._password = credentials["password"]
                    return True
        except yaml.YAMLError:
            pass

        return False

    def _get_config_dict(self) -> Dict[str, Any]:
        """Get configuration dictionary based on platform type."""
        config_map = {
            "Grid5000": lambda: self._create_g5k_config(self.BASE_ROLES),
            "VMonG5k": lambda: self._create_vmong5k_config(self.BASE_ROLES),
            "FIT": lambda: self._create_fit_config(self.BASE_ROLES),
            "VagrantG5k": lambda: self._create_custom_vagrantong5k_config(self.BASE_ROLES),
        }

        config_factory = config_map.get(self.platform_type)
        if not config_factory:
            raise EnosConfigurationError(f"Unsupported platform type: {self.platform_type}")

        return config_factory()

    def setup(self, verbose: bool = False) -> Dict[str, Any]:
        """Setup the platform."""
        self.check_credentials_file()

        if verbose:
            en.check()

        return self._get_config_dict()

    def destroy(self) -> None:
        """Destroy the platform."""
        conf_dict = self._get_config_dict()
        provider = self.get_provider(self.platform_type, conf_dict)
        provider.destroy()

    @property
    def vm_groups(self) -> List[VMGroup]:
        """Return VM groups for VagrantG5k platforms."""
        return self._vm_groups

    @property
    def start_time(self) -> str:
        """Return start time from configuration."""
        return self._platform_config.get("start_time", "now")
