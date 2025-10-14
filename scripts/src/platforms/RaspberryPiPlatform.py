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

import subprocess
from typing import Dict, Any

import yaml

from scripts.src.platforms.Platform import Platform


class RaspberryPiConfigurationError(Exception):
    """Raised when RaspberryPi configuration is invalid."""

    pass


class RaspberryPiPlatform(Platform):
    """Platform implementation for Raspberry Pi devices."""

    def _validate_config(self) -> None:
        """Validate platform configuration."""
        if "inventory" not in self._platform_config:
            raise RaspberryPiConfigurationError("Missing 'inventory' field in configuration")

    def _test_ssh_connection(self, host: str) -> bool:
        """Test SSH connection to a host."""
        self._log.debuggg(f"Testing SSH connection to {host}")
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", host, "echo", "alive"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False

    def _load_hosts_from_inventory(self) -> Dict[str, Any]:
        """Load hosts from inventory file."""
        try:
            with open(self._platform_config["inventory"], "r") as file:
                inventory_data = yaml.safe_load(file)
                return inventory_data.get("pico", {}).get("hosts", {})
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise RaspberryPiConfigurationError(f"Error loading inventory file: {str(e)}")

    def _get_alive_hosts(self, hosts: Dict[str, Any]) -> list[str]:
        """Get list of hosts that are alive and reachable."""
        return [
            host for host in hosts if self._test_ssh_connection(hosts[host]["ansible_ssh_host"])
        ]

    def _validate_host_requirements(self, alive_hosts: list, required_total: int) -> None:
        """Validate that we have enough alive hosts."""
        if len(alive_hosts) < required_total:
            raise RaspberryPiConfigurationError(
                f"Not enough alive hosts: need {required_total}, found {len(alive_hosts)}"
            )

    def setup(self, verbose: bool = False) -> Dict[str, Any]:
        """Setup Raspberry Pi platform."""
        self._log.info("Setting up Raspberry Pi platform")

        hosts = self._load_hosts_from_inventory()

        required_producers = int(self._platform_config.get("producers", 0))
        required_consumers = int(self._platform_config.get("consumers", 0))
        required_total = required_producers + required_consumers

        inventory = {
            "producers": {"hosts": {}},
            "consumers": {"hosts": {}},
            "pico": {"hosts": {}},
            "agents": {"hosts": {}},
        }

        alive_hosts = self._get_alive_hosts(hosts)
        self._validate_host_requirements(alive_hosts, required_total)

        # Assign roles to hosts
        for i, host in enumerate(alive_hosts[:required_total]):
            host_config = hosts[host].copy()

            if i < required_producers:
                role = "producer"
                inventory["producers"]["hosts"][host] = host_config
            else:
                role = "consumer"
                inventory["consumers"]["hosts"][host] = host_config

            # Add to common inventories
            host_config["cluster_role"] = role
            inventory["pico"]["hosts"][host] = host_config
            inventory["agents"]["hosts"][host] = host_config

            self._log.debugg(f"Added {role} {host}")

        self._log.debugg(f"Final pi Inventory: {inventory}")
        return inventory

    def destroy(self) -> None:
        """Destroy Raspberry Pi platform (no-op)."""
        pass
