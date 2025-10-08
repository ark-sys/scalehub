"""Platform configuration fixtures for testing."""

from typing import Dict, Any


class PlatformConfigs:
    """Common platform configurations for testing."""

    @staticmethod
    def grid5000_config() -> Dict[str, Any]:
        """Grid5000 platform configuration."""
        return {
            "type": "Grid5000",
            "name": "test_grid5000",
            "reservation_name": "scalehub_test",
            "site": "rennes",
            "cluster": "paradoxe",
            "control": 1,
            "producers": 2,
            "consumers": 3,
            "queue": "default",
            "walltime": "01:00:00",
            "start_time": "now"
        }

    @staticmethod
    def vmong5k_config() -> Dict[str, Any]:
        """VMonG5k platform configuration."""
        return {
            "type": "VMonG5k",
            "name": "test_vmong5k",
            "reservation_name": "scalehub_vm_test",
            "site": "rennes",
            "cluster": "paradoxe",
            "control": 0,
            "producers": 1,
            "consumers": 2,
            "queue": "default",
            "walltime": "01:00:00",
            "start_time": "now",
            "core_per_vm": 4,
            "memory_per_vm": 8192,
            "disk_per_vm": 50
        }

    @staticmethod
    def vagrantg5k_config() -> Dict[str, Any]:
        """VagrantG5k platform configuration."""
        return {
            "type": "VagrantG5k",
            "name": "test_vagrant",
            "reservation_name": "scalehub_vagrant_test",
            "site": "rennes",
            "cluster": "paradoxe",
            "control": 0,
            "producers": 2,
            "consumers": 1,
            "queue": "default",
            "walltime": "01:00:00",
            "start_time": "now",
            "core_per_vm": 2,
            "memory_per_vm": 4096,
            "disk_per_vm": 30
        }

    @staticmethod
    def fit_config() -> Dict[str, Any]:
        """FIT platform configuration."""
        return {
            "type": "FIT",
            "name": "test_fit",
            "reservation_name": "scalehub_fit_test",
            "site": "grenoble",
            "cluster": "m3",
            "control": 0,
            "producers": 5,
            "consumers": 10,
            "queue": "default",
            "walltime": "01:00:00",
            "start_time": "now",
            "archi": "m3:at86rf231"
        }

    @staticmethod
    def raspberry_pi_config() -> Dict[str, Any]:
        """Raspberry Pi platform configuration."""
        return {
            "type": "RaspberryPi",
            "name": "test_raspberry_pi",
            "inventory": "/tmp/test_inventory.yaml",
            "producers": 2,
            "consumers": 3,
            "cluster": "pico2"
        }

    @staticmethod
    def invalid_config() -> Dict[str, Any]:
        """Invalid platform configuration for testing error cases."""
        return {
            "name": "invalid_platform"
            # Missing required 'type' field
        }

    @staticmethod
    def unsupported_config() -> Dict[str, Any]:
        """Unsupported platform configuration for testing error cases."""
        return {
            "type": "UnsupportedPlatform",
            "name": "unsupported_platform"
        }

    @staticmethod
    def provision_manager_config() -> Dict[str, Any]:
        """Configuration for ProvisionManager testing."""
        return {
            "scalehub": {
                "inventory": "/tmp/inventory"
            },
            "platforms": {
                "platforms": [
                    PlatformConfigs.grid5000_config(),
                    PlatformConfigs.raspberry_pi_config()
                ],
                "enable_ipv6": True
            }
        }