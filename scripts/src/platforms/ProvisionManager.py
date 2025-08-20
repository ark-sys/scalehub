import os
from typing import Dict, List, Any

import yaml

from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.EnosPlatforms import EnosPlatforms
from scripts.src.platforms.Platform import Platform
from scripts.src.platforms.PlatformFactory import PlatformFactory, PlatformCreationError
from scripts.src.platforms.RaspberryPiPlatform import RaspberryPiPlatform
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class ProvisionManagerError(Exception):
    """Raised when provisioning fails."""

    pass


class ProvisionManager:
    """Manager for platform provisioning operations."""

    def __init__(self, log: Logger, config):
        self._log = log
        self._config = config
        self._inventory_dict: Dict[str, Any] = {}

        self._platforms = self._create_platforms()
        self._enos_platforms = self._filter_enos_platforms()
        self._raspberry_pis = self._filter_raspberry_pi_platforms()
        self._custom_platforms = self._filter_custom_platforms()

        self._log_platform_summary()

    def _create_platforms(self) -> List[Platform]:
        """Create platform instances from configuration."""
        platforms = []
        # Get platforms list from config - this is already parsed as dictionaries
        platform_configs = self._config.get(Key.Platforms.platforms.key)

        if not platform_configs:
            self._log.info(
                "No platforms configured. Using default platform configurations."
            )
            platform_configs = []

        for platform_config in platform_configs:
            try:
                platform = PlatformFactory.create_platform(self._log, platform_config)
                if platform:
                    platforms.append(platform)
            except PlatformCreationError as e:
                self._log.error(f"Failed to create platform: {str(e)}")

        return platforms

    def _filter_enos_platforms(self) -> List[EnosPlatform]:
        """Filter Enos platforms from all platforms."""
        return [p for p in self._platforms if isinstance(p, EnosPlatform)]

    def _filter_raspberry_pi_platforms(self) -> List[RaspberryPiPlatform]:
        """Filter Raspberry Pi platforms from all platforms."""
        return [p for p in self._platforms if isinstance(p, RaspberryPiPlatform)]

    def _filter_custom_platforms(self) -> List[Platform]:
        """Filter custom platforms (non-Enos, non-RaspberryPi) from all platforms."""
        return [
            p
            for p in self._platforms
            if not isinstance(p, (EnosPlatform, RaspberryPiPlatform))
        ]

    def _log_platform_summary(self) -> None:
        """Log summary of platforms."""
        self._log.debugg("=" * 50)
        self._log.debugg(f"Total platforms: {len(self._platforms)}")
        self._log.debugg(f"Enos platforms: {len(self._enos_platforms)}")
        self._log.debugg(f"Raspberry Pi platforms: {len(self._raspberry_pis)}")
        self._log.debugg(f"Custom platforms: {len(self._custom_platforms)}")
        self._log.debugg("=" * 50)

    def _ensure_inventory_directory(self) -> None:
        """Ensure inventory directory exists."""
        inventory_dir = self._config.get(Key.Scalehub.inventory.key)
        os.makedirs(inventory_dir, exist_ok=True)

    def _save_inventory(self, inventory: Dict[str, Any], filename: str) -> str:
        """Save inventory to file and return the path."""
        inventory_path = os.path.join(
            self._config.get(Key.Scalehub.inventory.key), filename
        )

        with open(inventory_path, "w") as inventory_file:
            yaml.dump(inventory, inventory_file, default_flow_style=False)

        self._inventory_dict[inventory_path] = inventory
        return inventory_path

    def _provision_enos_platforms(self) -> None:
        """Provision Enos platforms."""
        if not self._enos_platforms:
            return

        self._log.debug("Found Enos platforms. Generating inventory.")

        try:
            enos_providers = EnosPlatforms(self._log, self._enos_platforms)
            enos_inventory = enos_providers.setup()

            if enos_inventory:
                self._save_inventory(enos_inventory, "enos_inventory.yaml")

                # Enable firewall if IPv6 is enabled
                if self._config.get_bool(Key.Platforms.enable_ipv6.key):
                    enos_providers.post_setup()
        except Exception as e:
            raise ProvisionManagerError(f"Failed to provision Enos platforms: {str(e)}")

    def _provision_raspberry_pi_platforms(self) -> None:
        """Provision Raspberry Pi platforms."""
        if not self._raspberry_pis:
            return

        self._log.debug("Found Raspberry Pi platforms. Generating inventory.")

        try:
            # Assume single Raspberry Pi platform for now
            pi_inventory = self._raspberry_pis[0].setup()
            self._save_inventory(pi_inventory, "pi_inventory.yaml")
        except Exception as e:
            raise ProvisionManagerError(
                f"Failed to provision Raspberry Pi platforms: {str(e)}"
            )

    def _provision_custom_platforms(self) -> None:
        """Provision custom platforms."""
        if not self._custom_platforms:
            return

        self._log.debug("Found custom platforms. Generating inventories.")

        for i, platform in enumerate(self._custom_platforms):
            try:
                self._log.info(
                    f"Provisioning custom platform: {platform.platform_name} ({platform.platform_type})"
                )
                custom_inventory = platform.setup()

                # Use platform type and name for filename
                filename = f"{platform.platform_type.lower()}_{platform.platform_name}_inventory.yaml"
                # Remove any invalid filename characters
                filename = "".join(c for c in filename if c.isalnum() or c in "._-")

                self._save_inventory(custom_inventory, filename)

            except Exception as e:
                raise ProvisionManagerError(
                    f"Failed to provision custom platform {platform.platform_name}: {str(e)}"
                )

    def provision(self) -> Dict[str, Any]:
        """Provision all platforms."""
        self._log.info("Provisioning platforms")
        self._ensure_inventory_directory()

        self._provision_enos_platforms()
        self._provision_raspberry_pi_platforms()
        self._provision_custom_platforms()

        if not self._inventory_dict:
            raise ProvisionManagerError(
                "No platforms are specified in the configuration file."
            )

        self._log.info("Provisioning completed.")
        return self._inventory_dict

    def destroy(self) -> None:
        """Destroy all platforms."""
        self._log.info("Destroying platforms")

        # Destroy Enos platforms
        if self._enos_platforms:
            try:
                enos_providers = EnosPlatforms(self._log, self._enos_platforms)
                enos_providers.destroy()
            except Exception as e:
                self._log.error(f"Error destroying Enos platforms: {str(e)}")

        # Destroy custom platforms
        for platform in self._custom_platforms:
            try:
                self._log.info(f"Destroying custom platform: {platform.platform_name}")
                platform.destroy()
            except Exception as e:
                self._log.error(
                    f"Error destroying custom platform {platform.platform_name}: {str(e)}"
                )

        # Raspberry Pi platforms don't need explicit destruction
        self._log.info("Platform destruction completed.")
