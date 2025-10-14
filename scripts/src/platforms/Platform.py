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

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class Platform(ABC):
    """Abstract base class for all platform implementations."""
    
    def __init__(self, log, platform_config: Dict[str, Any]):
        self._log = log
        self._platform_config = platform_config
        self._validate_config()
    
    @abstractmethod
    def _validate_config(self) -> None:
        """Validate platform-specific configuration."""
        pass
    
    @abstractmethod
    def setup(self, verbose: bool = False) -> Optional[Dict[str, Any]]:
        """Setup the platform and return inventory or configuration."""
        pass

    @abstractmethod
    def destroy(self) -> None:
        """Destroy the platform resources."""
        pass
    
    @property
    def platform_type(self) -> str:
        """Return the platform type."""
        return self._platform_config.get("type", "unknown")
    
    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return self._platform_config.get("name", "unnamed")
    
    @property
    def platform_config(self) -> Dict[str, Any]:
        """Return the platform configuration."""
        return self._platform_config
