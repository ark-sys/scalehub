import pytest
from unittest.mock import MagicMock, patch, mock_open
from scripts.src.platforms.RaspberryPiPlatform import RaspberryPiPlatform, RaspberryPiConfigurationError


@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def valid_config():
    return {
        "type": "RaspberryPi",
        "inventory": "/path/to/inventory.yaml",
        "producers": 2,
        "consumers": 3,
    }


@pytest.fixture
def inventory_data():
    return {
        "pico": {
            "hosts": {
                "pi1": {"ansible_ssh_host": "192.168.1.1"},
                "pi2": {"ansible_ssh_host": "192.168.1.2"},
                "pi3": {"ansible_ssh_host": "192.168.1.3"},
                "pi4": {"ansible_ssh_host": "192.168.1.4"},
                "pi5": {"ansible_ssh_host": "192.168.1.5"},
            }
        }
    }


def test_validate_config_success(logger_mock, valid_config):
    """Test successful validation of configuration."""
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    assert platform.platform_config == valid_config


def test_validate_config_failure(logger_mock):
    """Test validation failure when 'inventory' is missing."""
    invalid_config = {"type": "RaspberryPi"}
    with pytest.raises(RaspberryPiConfigurationError, match="Missing 'inventory' field in configuration"):
        RaspberryPiPlatform(logger_mock, invalid_config)


@patch("builtins.open", new_callable=mock_open, read_data="pico:\n  hosts:\n    pi1:\n      ansible_ssh_host: 192.168.1.1")
@patch("yaml.safe_load")
def test_load_hosts_from_inventory(mock_yaml_load, mock_file, logger_mock, valid_config, inventory_data):
    """Test loading hosts from inventory file."""
    mock_yaml_load.return_value = inventory_data
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    hosts = platform._load_hosts_from_inventory()

    assert hosts == inventory_data["pico"]["hosts"]
    mock_file.assert_called_once_with("/path/to/inventory.yaml", "r")
    mock_yaml_load.assert_called_once()


@patch("scripts.src.platforms.RaspberryPiPlatform.RaspberryPiPlatform._test_ssh_connection")
def test_get_alive_hosts(mock_ssh, logger_mock, valid_config, inventory_data):
    """Test filtering alive hosts."""
    mock_ssh.side_effect = lambda host: host in ["192.168.1.1", "192.168.1.2", "192.168.1.3"]
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    alive_hosts = platform._get_alive_hosts(inventory_data["pico"]["hosts"])

    assert alive_hosts == ["pi1", "pi2", "pi3"]
    mock_ssh.assert_any_call("192.168.1.1")
    mock_ssh.assert_any_call("192.168.1.2")
    mock_ssh.assert_any_call("192.168.1.3")


def test_validate_host_requirements_success(logger_mock, valid_config):
    """Test successful validation of host requirements."""
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    platform._validate_host_requirements(["pi1", "pi2", "pi3", "pi4", "pi5"], 5)


def test_validate_host_requirements_failure(logger_mock, valid_config):
    """Test failure when not enough alive hosts are available."""
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    with pytest.raises(RaspberryPiConfigurationError, match="Not enough alive hosts: need 5, found 3"):
        platform._validate_host_requirements(["pi1", "pi2", "pi3"], 5)


@patch("scripts.src.platforms.RaspberryPiPlatform.RaspberryPiPlatform._load_hosts_from_inventory")
@patch("scripts.src.platforms.RaspberryPiPlatform.RaspberryPiPlatform._get_alive_hosts")
@patch("scripts.src.platforms.RaspberryPiPlatform.RaspberryPiPlatform._validate_host_requirements")
def test_setup(mock_validate, mock_alive, mock_load, logger_mock, valid_config, inventory_data):
    """Test the setup method."""
    mock_load.return_value = inventory_data["pico"]["hosts"]
    mock_alive.return_value = ["pi1", "pi2", "pi3", "pi4", "pi5"]

    platform = RaspberryPiPlatform(logger_mock, valid_config)
    inventory = platform.setup()

    assert "producers" in inventory
    assert "consumers" in inventory
    assert len(inventory["producers"]["hosts"]) == 2
    assert len(inventory["consumers"]["hosts"]) == 3
    mock_load.assert_called_once()
    mock_alive.assert_called_once_with(inventory_data["pico"]["hosts"])
    mock_validate.assert_called_once_with(["pi1", "pi2", "pi3", "pi4", "pi5"], 5)


def test_destroy(logger_mock, valid_config):
    """Test the destroy method (no-op)."""
    platform = RaspberryPiPlatform(logger_mock, valid_config)
    platform.destroy()  # Should not raise any exceptions