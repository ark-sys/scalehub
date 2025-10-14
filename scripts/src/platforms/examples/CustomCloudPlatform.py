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

"""
Example custom platform implementation for educational purposes.
This demonstrates how to create a custom platform that integrates with Scalehub.
"""

import time
from typing import Dict, Any

from scripts.src.platforms.Platform import Platform


class CustomCloudConfigurationError(Exception):
    """Raised when CustomCloud configuration is invalid."""

    pass


class CustomCloudPlatform(Platform):
    """Example custom platform implementation for a hypothetical cloud provider."""

    def _validate_config(self) -> None:
        """Validate platform configuration."""
        required_fields = ["api_endpoint", "region", "instance_type"]
        for field in required_fields:
            if field not in self._platform_config:
                raise CustomCloudConfigurationError(f"Missing required field: {field}")

        # Validate specific field values
        if not self._platform_config.get("api_endpoint", "").startswith("http"):
            raise CustomCloudConfigurationError("api_endpoint must be a valid HTTP/HTTPS URL")

    def _get_instance_count(self) -> int:
        """Calculate total instance count from INI configuration."""
        # Get counts from INI configuration (similar to Grid5000 pattern)
        control_count = int(self._platform_config.get("control", 0))
        producer_count = int(self._platform_config.get("producers", 0))
        consumer_count = int(self._platform_config.get("consumers", 0))

        # If instance_count is explicitly set, use that instead
        if "instance_count" in self._platform_config:
            return int(self._platform_config["instance_count"])

        # Otherwise calculate from role counts
        total_count = control_count + producer_count + consumer_count
        return max(total_count, 1)  # Ensure at least 1 instance

    def _provision_instances(self) -> list[Dict[str, Any]]:
        """Simulate instance provisioning."""
        self._log.info("Provisioning instances on CustomCloud...")

        # Simulate API call delay
        time.sleep(2)

        # Mock instance data
        instances = []
        instance_count = self._get_instance_count()

        self._log.debug(f"Provisioning {instance_count} instances")

        for i in range(instance_count):
            instances.append(
                {
                    "id": f"instance-{i+1}",
                    "ip": f"192.168.1.{i+10}",
                    "region": self._platform_config["region"],
                    "type": self._platform_config["instance_type"],
                    "name": f"{self.platform_name}-{i+1}",
                }
            )

        return instances

    def _create_inventory(self, instances: list[Dict[str, Any]]) -> Dict[str, Any]:
        """Create Ansible inventory from provisioned instances."""
        inventory = {
            "control": {"hosts": {}},
            "agents": {"hosts": {}},
            "customcloud": {"hosts": {}},
        }

        # Get role counts from configuration
        control_count = int(self._platform_config.get("control", 1))
        producer_count = int(self._platform_config.get("producers", 0))
        consumer_count = int(self._platform_config.get("consumers", 0))

        # SSH configuration from platform config
        ssh_user = self._platform_config.get("ssh_user", "ubuntu")
        ssh_key = self._platform_config.get("ssh_key_path", "~/.ssh/id_rsa")

        instance_index = 0

        # Assign control nodes
        for i in range(control_count):
            if instance_index >= len(instances):
                break

            instance = instances[instance_index]
            hostname = f"control-{instance['id']}"

            host_config = {
                "ansible_host": instance["ip"],
                "ansible_user": ssh_user,
                "ansible_ssh_private_key_file": ssh_key,
                "cluster_role": "control",
                "instance_id": instance["id"],
                "region": instance["region"],
                "instance_type": instance["type"],
                "platform_name": self.platform_name,
            }

            inventory["control"]["hosts"][hostname] = host_config
            inventory["customcloud"]["hosts"][hostname] = host_config
            instance_index += 1

        # Assign producer nodes
        for i in range(producer_count):
            if instance_index >= len(instances):
                break

            instance = instances[instance_index]
            hostname = f"producer-{instance['id']}"

            host_config = {
                "ansible_host": instance["ip"],
                "ansible_user": ssh_user,
                "ansible_ssh_private_key_file": ssh_key,
                "cluster_role": "producer",
                "instance_id": instance["id"],
                "region": instance["region"],
                "instance_type": instance["type"],
                "platform_name": self.platform_name,
            }

            inventory["agents"]["hosts"][hostname] = host_config
            inventory["customcloud"]["hosts"][hostname] = host_config
            instance_index += 1

        # Assign consumer nodes
        for i in range(consumer_count):
            if instance_index >= len(instances):
                break

            instance = instances[instance_index]
            hostname = f"consumer-{instance['id']}"

            host_config = {
                "ansible_host": instance["ip"],
                "ansible_user": ssh_user,
                "ansible_ssh_private_key_file": ssh_key,
                "cluster_role": "consumer",
                "instance_id": instance["id"],
                "region": instance["region"],
                "instance_type": instance["type"],
                "platform_name": self.platform_name,
            }

            inventory["agents"]["hosts"][hostname] = host_config
            inventory["customcloud"]["hosts"][hostname] = host_config
            instance_index += 1

        # Assign any remaining instances as generic workers
        for i in range(instance_index, len(instances)):
            instance = instances[i]
            hostname = f"worker-{instance['id']}"

            host_config = {
                "ansible_host": instance["ip"],
                "ansible_user": ssh_user,
                "ansible_ssh_private_key_file": ssh_key,
                "cluster_role": "worker",
                "instance_id": instance["id"],
                "region": instance["region"],
                "instance_type": instance["type"],
                "platform_name": self.platform_name,
            }

            inventory["agents"]["hosts"][hostname] = host_config
            inventory["customcloud"]["hosts"][hostname] = host_config

        return inventory

    def setup(self, verbose: bool = False) -> Dict[str, Any]:
        """Setup the CustomCloud platform."""
        self._log.info(f"Setting up CustomCloud platform: {self.platform_name}")

        if verbose:
            self._log.debug(f"Platform config: {self._platform_config}")

        # Provision instances
        instances = self._provision_instances()

        # Create inventory
        inventory = self._create_inventory(instances)

        self._log.info(f"CustomCloud platform setup completed with {len(instances)} instances")
        return inventory

    def destroy(self) -> None:
        """Destroy CustomCloud platform resources."""
        self._log.info(f"Destroying CustomCloud platform: {self.platform_name}")

        # Simulate cleanup operations
        time.sleep(1)

        self._log.info("CustomCloud platform destroyed")


# Example of how to register the custom platform
def register_custom_platform():
    """Example function showing how to register the custom platform."""
    from scripts.src.platforms.PlatformFactory import PlatformFactory

    # Register the custom platform
    PlatformFactory.register_platform("CustomCloud", CustomCloudPlatform)
