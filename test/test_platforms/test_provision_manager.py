import pytest
from unittest.mock import MagicMock, patch, call
from scripts.src.platforms.ProvisionManager import ProvisionManager, ProvisionManagerError
from scripts.src.platforms.PlatformFactory import PlatformFactory
from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.RaspberryPiPlatform import RaspberryPiPlatform


@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def config_mock():
    return {
        "platforms": [
            {"type": "Grid5000", "reservation_name": "test_reservation", "site": "rennes"},
            {"type": "RaspberryPi", "inventory": "/path/to/inventory.yaml"},
        ],
        "scalehub": {"inventory": "/tmp/inventory"},
    }


@patch("scripts.src.platforms.ProvisionManager.PlatformFactory.create_platform")
def test_create_platforms(mock_create_platform, logger_mock, config_mock):
    """Test the _create_platforms method."""
    mock_create_platform.side_effect = [
        MagicMock(spec=EnosPlatform),
        MagicMock(spec=RaspberryPiPlatform),
    ]
    manager = ProvisionManager(logger_mock, config_mock)
    platforms = manager._create_platforms()

    assert len(platforms) == 2
    mock_create_platform.assert_has_calls([
        call(logger_mock, config_mock["platforms"][0]),
        call(logger_mock, config_mock["platforms"][1]),
    ])


@patch("os.makedirs")
def test_ensure_inventory_directory(mock_makedirs, logger_mock, config_mock):
    """Test the _ensure_inventory_directory method."""
    manager = ProvisionManager(logger_mock, config_mock)
    manager._ensure_inventory_directory()

    mock_makedirs.assert_called_once_with(config_mock["scalehub"]["inventory"], exist_ok=True)


@patch("builtins.open", new_callable=MagicMock)
@patch("yaml.dump")
def test_save_inventory(mock_yaml_dump, mock_open, logger_mock, config_mock):
    """Test the _save_inventory method."""
    manager = ProvisionManager(logger_mock, config_mock)
    inventory = {"test": "data"}
    path = manager._save_inventory(inventory, "test_inventory.yaml")

    assert path == "/tmp/inventory/test_inventory.yaml"
    mock_open.assert_called_once_with("/tmp/inventory/test_inventory.yaml", "w")
    mock_yaml_dump.assert_called_once_with(inventory, mock_open.return_value.__enter__(), default_flow_style=False)


@patch("scripts.src.platforms.ProvisionManager.EnosPlatforms")
def test_provision_enos_platforms(mock_enos_platforms, logger_mock, config_mock):
    """Test the _provision_enos_platforms method."""
    enos_mock = mock_enos_platforms.return_value
    enos_mock.setup.return_value = {"inventory": "data"}

    manager = ProvisionManager(logger_mock, config_mock)
    manager._enos_platforms = [MagicMock(spec=EnosPlatform)]
    manager._provision_enos_platforms()

    enos_mock.setup.assert_called_once()
    logger_mock.debug.assert_called_with("Found Enos platforms. Generating inventory.")


@patch("scripts.src.platforms.ProvisionManager.RaspberryPiPlatform.setup")
def test_provision_raspberry_pi_platforms(mock_setup, logger_mock, config_mock):
    """Test the _provision_raspberry_pi_platforms method."""
    mock_setup.return_value = {"inventory": "data"}

    manager = ProvisionManager(logger_mock, config_mock)
    manager._raspberry_pis = [MagicMock(spec=RaspberryPiPlatform)]
    manager._provision_raspberry_pi_platforms()

    mock_setup.assert_called_once()
    logger_mock.debug.assert_called_with("Found Raspberry Pi platforms. Generating inventory.")


@patch("scripts.src.platforms.ProvisionManager.ProvisionManager._provision_enos_platforms")
@patch("scripts.src.platforms.ProvisionManager.ProvisionManager._provision_raspberry_pi_platforms")
@patch("scripts.src.platforms.ProvisionManager.ProvisionManager._provision_custom_platforms")
def test_provision(mock_custom, mock_raspberry, mock_enos, logger_mock, config_mock):
    """Test the provision method."""
    manager = ProvisionManager(logger_mock, config_mock)
    manager.provision()

    mock_enos.assert_called_once()
    mock_raspberry.assert_called_once()
    mock_custom.assert_called_once()
    logger_mock.info.assert_called_with("Provisioning platforms")


@patch("scripts.src.platforms.ProvisionManager.EnosPlatforms.destroy")
def test_destroy(mock_destroy, logger_mock, config_mock):
    """Test the destroy method."""
    manager = ProvisionManager(logger_mock, config_mock)
    manager._enos_platforms = [MagicMock(spec=EnosPlatform)]
    manager.destroy()

    mock_destroy.assert_called_once()
    logger_mock.info.assert_called_with("Destroying platforms")