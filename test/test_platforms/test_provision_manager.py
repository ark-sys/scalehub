from unittest.mock import MagicMock, patch, call, mock_open

import pytest

from src.scalehub.platforms.EnosPlatform import EnosPlatform
from src.scalehub.platforms.ProvisionManager import (
    ProvisionManager,
)
from src.scalehub.platforms.RaspberryPiPlatform import RaspberryPiPlatform


@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def config_mock():
    return {
        "platforms": [
            {
                "type": "Grid5000",
                "reservation_name": "test_reservation",
                "site": "rennes",
            },
            {"type": "RaspberryPi", "inventory": "/path/to/inventory.yaml"},
        ],
        "scalehub": {"inventory": "/tmp/inventory"},
    }


@patch("src.scalehub.platforms.ProvisionManager.PlatformFactory.create_platform")
def test_create_platforms(mock_create_platform, logger_mock, config_mock):
    """Test the _create_platforms method."""
    mock_platform1 = MagicMock(spec=EnosPlatform)
    mock_platform2 = MagicMock(spec=RaspberryPiPlatform)

    mock_create_platform.side_effect = [mock_platform1, mock_platform2]

    manager = ProvisionManager(logger_mock, config_mock)

    # Test the platforms created during initialization instead of calling the method again
    assert len(manager._platforms) == 2
    assert mock_create_platform.call_count == 2


@patch("os.makedirs")
@patch("src.scalehub.platforms.ProvisionManager.Key")
def test_ensure_inventory_directory(mock_key, mock_makedirs, logger_mock, config_mock):
    """Test the _ensure_inventory_directory method."""
    mock_key.Scalehub.inventory.key = "scalehub.inventory"

    # Update config_mock to use flattened key structure
    config_mock["scalehub.inventory"] = "/tmp/inventory"

    manager = ProvisionManager(logger_mock, config_mock)
    manager._ensure_inventory_directory()

    mock_makedirs.assert_called_once_with("/tmp/inventory", exist_ok=True)


@patch("builtins.open", new_callable=MagicMock)
@patch("yaml.dump")
@patch("src.scalehub.platforms.ProvisionManager.Key")
def test_save_inventory(mock_key, mock_yaml_dump, mock_open, logger_mock, config_mock):
    """Test the _save_inventory method."""
    # Mock the key to return the correct config key
    mock_key.Scalehub.inventory.key = "scalehub.inventory"

    # Add the flattened key to config
    config_mock["scalehub.inventory"] = "/tmp/inventory"

    manager = ProvisionManager(logger_mock, config_mock)
    inventory = {"test": "data"}
    path = manager._save_inventory(inventory, "test_inventory.yaml")

    assert path == "/tmp/inventory/test_inventory.yaml"
    mock_open.assert_called_once_with("/tmp/inventory/test_inventory.yaml", "w")
    mock_yaml_dump.assert_called_once_with(
        inventory, mock_open.return_value.__enter__(), default_flow_style=False
    )


@patch("src.scalehub.platforms.ProvisionManager.EnosPlatforms")
@patch("src.scalehub.platforms.ProvisionManager.Key")
@patch("src.scalehub.platforms.ProvisionManager.PlatformFactory.create_platform")
@patch("builtins.open", new_callable=MagicMock)
@patch("yaml.dump")
@patch("src.scalehub.platforms.ProvisionManager.os.makedirs")
def test_provision_enos_platforms(
    mock_makedirs,
    mock_yaml_dump,
    mock_open,
    mock_create_platform,
    mock_key,
    mock_enos_platforms,
    logger_mock,
    config_mock,
):
    """Test the _provision_enos_platforms method."""
    # Mock the key to return the correct config key
    mock_key.Scalehub.inventory.key = "scalehub.inventory"
    mock_key.Platforms.platforms.key = "platforms"
    mock_key.Platforms.enable_ipv6.key = "platforms.enable_ipv6"

    # Fix the platforms structure in config_mock to have proper dictionaries
    config_mock["platforms"] = [
        {"reservation_name": "test_reservation", "site": "rennes", "type": "Grid5000"},
        {"inventory": "/path/to/inventory.yaml", "type": "RaspberryPi"},
    ]
    config_mock["platforms.enable_ipv6"] = "false"
    config_mock["scalehub.inventory"] = "/tmp/inventory"

    # Create mock platforms - EnosPlatform for first, RaspberryPi for second
    mock_enos_platform = MagicMock(spec=EnosPlatform)
    mock_raspberry_platform = MagicMock(spec=RaspberryPiPlatform)
    mock_create_platform.side_effect = [mock_enos_platform, mock_raspberry_platform]

    # Create a proper config mock object with get and get_bool methods
    config_mock_obj = MagicMock()
    config_mock_obj.get.side_effect = lambda key, default=None: config_mock.get(key, default)
    config_mock_obj.get_bool.return_value = False

    enos_mock = mock_enos_platforms.return_value
    enos_mock.setup.return_value = {"inventory": "data"}

    manager = ProvisionManager(logger_mock, config_mock_obj)
    manager._provision_enos_platforms()

    # Verify calls
    mock_makedirs.assert_not_called()
    mock_enos_platforms.assert_called_once_with(logger_mock, [mock_enos_platform])
    enos_mock.setup.assert_called_once()
    mock_yaml_dump.assert_called_once()


@patch("builtins.open", new_callable=mock_open)
@patch("src.scalehub.platforms.ProvisionManager.yaml.dump")
@patch("src.scalehub.platforms.ProvisionManager.os.makedirs")
def test_provision_raspberry_pi_platforms(
    mock_makedirs, mock_yaml_dump, mock_file, logger_mock, config_mock
):
    """Test the _provision_raspberry_pi_platforms method."""
    # Add the flattened key that _save_inventory expects
    config_mock["scalehub.inventory"] = "/tmp/inventory"

    manager = ProvisionManager(logger_mock, config_mock)

    # Create a mock RaspberryPi platform and configure its setup method
    mock_pi = MagicMock(spec=RaspberryPiPlatform)
    mock_pi.setup.return_value = {"inventory": "data"}
    manager._raspberry_pis = [mock_pi]

    manager._provision_raspberry_pi_platforms()

    # Verify setup was called
    mock_pi.setup.assert_called_once()

    # Verify file operations
    mock_file.assert_called_once_with("/tmp/inventory/pi_inventory.yaml", "w")
    mock_yaml_dump.assert_called_once_with(
        {"inventory": "data"},
        mock_file.return_value.__enter__.return_value,
        default_flow_style=False,
    )


@patch("src.scalehub.platforms.ProvisionManager.ProvisionManager._provision_enos_platforms")
@patch("src.scalehub.platforms.ProvisionManager.ProvisionManager._provision_raspberry_pi_platforms")
@patch("src.scalehub.platforms.ProvisionManager.ProvisionManager._provision_custom_platforms")
@patch("src.scalehub.platforms.ProvisionManager.Key")
@patch("os.makedirs")
def test_provision(
    mock_makedirs,
    mock_key,
    mock_custom,
    mock_raspberry,
    mock_enos,
    logger_mock,
    config_mock,
):
    """Test the provision method."""
    # Mock the key to return the correct config key
    mock_key.Scalehub.inventory.key = "scalehub.inventory"

    # Add the flattened key to config
    config_mock["scalehub.inventory"] = "/tmp/inventory"

    manager = ProvisionManager(logger_mock, config_mock)

    # Configure the mocks to directly modify the manager's inventory dict
    def mock_enos_provision():
        manager._inventory_dict["/tmp/inventory/enos_inventory.yaml"] = {"test": "enos_data"}

    def mock_raspberry_provision():
        manager._inventory_dict["/tmp/inventory/pi_inventory.yaml"] = {"test": "pi_data"}

    def mock_custom_provision():
        manager._inventory_dict["/tmp/inventory/custom_inventory.yaml"] = {"test": "custom_data"}

    mock_enos.side_effect = mock_enos_provision
    mock_raspberry.side_effect = mock_raspberry_provision
    mock_custom.side_effect = mock_custom_provision

    result = manager.provision()

    # Verify the methods were called
    mock_enos.assert_called_once()
    mock_raspberry.assert_called_once()
    mock_custom.assert_called_once()
    mock_makedirs.assert_called_once_with("/tmp/inventory", exist_ok=True)
    logger_mock.info.assert_any_call("Provisioning platforms")
    logger_mock.info.assert_any_call("Provisioning completed.")

    # Verify result contains the mocked inventory data
    assert len(result) == 3
    assert "/tmp/inventory/enos_inventory.yaml" in result
    assert "/tmp/inventory/pi_inventory.yaml" in result
    assert "/tmp/inventory/custom_inventory.yaml" in result


@patch("src.scalehub.platforms.ProvisionManager.EnosPlatforms.destroy")
def test_destroy(mock_destroy, logger_mock, config_mock):
    """Test the destroy method."""
    manager = ProvisionManager(logger_mock, config_mock)
    manager._enos_platforms = [MagicMock(spec=EnosPlatform)]
    manager.destroy()

    mock_destroy.assert_called_once()

    # Check that both log messages were called
    expected_calls = [
        call("Destroying platforms"),
        call("Platform destruction completed."),
    ]
    logger_mock.info.assert_has_calls(expected_calls)
