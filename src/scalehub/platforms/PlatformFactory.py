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

from typing import Dict, Any, Optional, Type

from src.scalehub.platforms.EnosPlatform import EnosPlatform
from src.scalehub.platforms.Platform import Platform
from src.scalehub.platforms.RaspberryPiPlatform import RaspberryPiPlatform
from src.utils.Logger import Logger


class PlatformCreationError(Exception):
    """Raised when platform creation fails."""

    pass


class PlatformFactory:
    """Factory for creating platform instances."""

    _PLATFORM_TYPES: Dict[str, Type[Platform]] = {
        "Grid5000": EnosPlatform,
        "VMonG5k": EnosPlatform,
        "FIT": EnosPlatform,
        "VagrantG5k": EnosPlatform,
        "RaspberryPi": RaspberryPiPlatform,
    }

    @classmethod
    def register_platform(cls, platform_type: str, platform_class: Type[Platform]) -> None:
        """Register a new platform type.

        Args:
            platform_type: The platform type identifier
            platform_class: The platform class that inherits from Platform

        Raises:
            ValueError: If platform_class doesn't inherit from Platform
        """
        if not issubclass(platform_class, Platform):
            raise ValueError(f"Platform class {platform_class.__name__} must inherit from Platform")

        cls._PLATFORM_TYPES[platform_type] = platform_class

    @classmethod
    def unregister_platform(cls, platform_type: str) -> None:
        """Unregister a platform type."""
        cls._PLATFORM_TYPES.pop(platform_type, None)

    @classmethod
    def create_platform(cls, log: Logger, platform_config: Dict[str, Any]) -> Optional[Platform]:
        """Create a platform instance based on configuration."""
        platform_type = platform_config.get("type")

        if not platform_type:
            raise PlatformCreationError("Platform type not specified in configuration")

        platform_class = cls._PLATFORM_TYPES.get(platform_type)
        if not platform_class:
            available_types = ", ".join(cls.get_supported_types())
            raise PlatformCreationError(
                f"Unsupported platform type: {platform_type}. "
                f"Available types: {available_types}"
            )

        try:
            return platform_class(log, platform_config)
        except Exception as e:
            raise PlatformCreationError(f"Failed to create platform {platform_type}: {str(e)}")

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Return list of supported platform types."""
        return list(cls._PLATFORM_TYPES.keys())

    @classmethod
    def is_supported(cls, platform_type: str) -> bool:
        """Check if a platform type is supported."""
        return platform_type in cls._PLATFORM_TYPES
