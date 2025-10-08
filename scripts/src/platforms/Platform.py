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
