from unittest.mock import MagicMock

import pytest

from src.scalehub.platforms.EnosPlatform import EnosPlatform
from src.scalehub.platforms.Platform import Platform
from src.scalehub.platforms.PlatformFactory import PlatformFactory, PlatformCreationError
from src.scalehub.platforms.RaspberryPiPlatform import RaspberryPiPlatform


@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def valid_enos_config():
    return {
        "type": "Grid5000",
        "reservation_name": "test_reservation",
        "walltime": "02:00:00",
        "queue": "default",
        "site": "rennes",
        "cluster": "paradoxe",
        "producers": 2,
        "consumers": 3,
    }


@pytest.fixture
def valid_raspberrypi_config():
    return {
        "type": "RaspberryPi",
        "inventory": "/path/to/inventory.yaml",
        "producers": 1,
        "consumers": 2,
    }


def test_create_platform_enos(logger_mock, valid_enos_config):
    """Test creating an EnosPlatform instance."""
    platform = PlatformFactory.create_platform(logger_mock, valid_enos_config)
    assert isinstance(platform, EnosPlatform)
    assert platform.platform_type == "Grid5000"


def test_create_platform_raspberrypi(logger_mock, valid_raspberrypi_config):
    """Test creating a RaspberryPiPlatform instance."""
    platform = PlatformFactory.create_platform(logger_mock, valid_raspberrypi_config)
    assert isinstance(platform, RaspberryPiPlatform)
    assert platform.platform_type == "RaspberryPi"


def test_create_platform_unsupported_type(logger_mock):
    """Test error when creating a platform with an unsupported type."""
    invalid_config = {"type": "UnsupportedPlatform"}
    with pytest.raises(PlatformCreationError, match="Unsupported platform type"):
        PlatformFactory.create_platform(logger_mock, invalid_config)


def test_create_platform_missing_type(logger_mock):
    """Test error when creating a platform with a missing type."""
    invalid_config = {}
    with pytest.raises(PlatformCreationError, match="Platform type not specified"):
        PlatformFactory.create_platform(logger_mock, invalid_config)


def test_register_new_platform_type():
    """Test registering a new platform type."""

    class MockPlatform(Platform):
        def _validate_config(self):
            pass

        def setup(self, verbose: bool = False):
            return {}

        def destroy(self):
            pass

    PlatformFactory.register_platform("MockPlatform", MockPlatform)
    assert PlatformFactory.is_supported("MockPlatform")

    # Cleanup
    PlatformFactory.unregister_platform("MockPlatform")


def test_unregister_platform_type():
    """Test unregistering a platform type."""
    PlatformFactory.unregister_platform("RaspberryPi")
    assert not PlatformFactory.is_supported("RaspberryPi")

    # Re-register for future tests
    PlatformFactory.register_platform("RaspberryPi", RaspberryPiPlatform)
