"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import MagicMock, patch
from test.fixtures.platform_configs import PlatformConfigs
from test.fixtures.mock_data import MockData


@pytest.fixture
def logger_mock():
    """Mock logger for testing."""
    logger = MagicMock()
    logger.debug = MagicMock()
    logger.debugg = MagicMock()
    logger.debuggg = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    return logger


@pytest.fixture
def grid5000_config():
    """Grid5000 platform configuration."""
    return PlatformConfigs.grid5000_config()


@pytest.fixture
def vmong5k_config():
    """VMonG5k platform configuration."""
    return PlatformConfigs.vmong5k_config()


@pytest.fixture
def vagrantg5k_config():
    """VagrantG5k platform configuration."""
    return PlatformConfigs.vagrantg5k_config()


@pytest.fixture
def fit_config():
    """FIT platform configuration."""
    return PlatformConfigs.fit_config()


@pytest.fixture
def raspberry_pi_config():
    """Raspberry Pi platform configuration."""
    return PlatformConfigs.raspberry_pi_config()


@pytest.fixture
def invalid_config():
    """Invalid platform configuration."""
    return PlatformConfigs.invalid_config()


@pytest.fixture
def unsupported_config():
    """Unsupported platform configuration."""
    return PlatformConfigs.unsupported_config()


@pytest.fixture
def provision_manager_config():
    """Configuration for ProvisionManager testing."""
    return PlatformConfigs.provision_manager_config()


@pytest.fixture
def raspberry_pi_inventory():
    """Mock Raspberry Pi inventory data."""
    return MockData.raspberry_pi_inventory()


@pytest.fixture
def enos_inventory():
    """Mock Enos inventory data."""
    return MockData.enos_inventory()


@pytest.fixture
def vagrant_inventory():
    """Mock Vagrant inventory data."""
    return MockData.vagrant_inventory()


@pytest.fixture
def vm_groups():
    """Mock VM groups for Vagrant platforms."""
    return MockData.vm_groups()


@pytest.fixture
def grid5000_api_nodes_response():
    """Mock Grid5000 API response for nodes."""
    return MockData.grid5000_api_nodes_response()


@pytest.fixture
def mock_enos_roles():
    """Mock Enos roles with host objects."""
    return MockData.mock_enos_roles()


@pytest.fixture
def mock_vm_roles():
    """Mock VM roles with host objects."""
    return MockData.mock_vm_roles()


@pytest.fixture
def expected_raspberry_pi_inventory():
    """Expected Raspberry Pi inventory after setup."""
    return MockData.expected_raspberry_pi_inventory()


@pytest.fixture
def mock_platform():
    """Mock platform instance."""
    platform = MagicMock()
    platform.platform_type = "Grid5000"
    platform.platform_name = "test_platform"
    platform.platform_config = PlatformConfigs.grid5000_config()
    platform.setup.return_value = {"resources": {"machines": []}}
    platform.destroy.return_value = None
    return platform


@pytest.fixture
def mock_enos_platform():
    """Mock Enos platform instance."""
    platform = MagicMock()
    platform.platform_type = "Grid5000"
    platform.platform_name = "test_grid5000"
    platform.platform_config = PlatformConfigs.grid5000_config()
    platform.start_time = "now"
    platform.setup.return_value = {
        "resources": {
            "machines": [
                {
                    "roles": ["producers", "test_grid5000", "baremetal"],
                    "cluster": "paradoxe",
                    "nodes": 2,
                }
            ]
        }
    }
    platform.get_provider.return_value = MagicMock()
    return platform


@pytest.fixture
def mock_raspberry_pi_platform():
    """Mock Raspberry Pi platform instance."""
    platform = MagicMock()
    platform.platform_type = "RaspberryPi"
    platform.platform_name = "test_raspberry_pi"
    platform.platform_config = PlatformConfigs.raspberry_pi_config()
    platform.setup.return_value = MockData.expected_raspberry_pi_inventory()
    platform.destroy.return_value = None
    return platform


@pytest.fixture
def mock_vagrant_platform():
    """Mock Vagrant platform instance."""
    platform = MagicMock()
    platform.platform_type = "VagrantG5k"
    platform.platform_name = "test_vagrant"
    platform.platform_config = PlatformConfigs.vagrantg5k_config()
    platform.start_time = "now"
    platform.vm_groups = MockData.vm_groups()
    platform.setup.return_value = {
        "resources": {
            "machines": [
                {
                    "roles": ["producers", "test_vagrant", "vagrant"],
                    "cluster": "paradoxe",
                    "nodes": 1,
                }
            ]
        }
    }
    platform.get_provider.return_value = MagicMock()
    return platform


@pytest.fixture
def mock_multiple_platforms(mock_enos_platform, mock_raspberry_pi_platform):
    """Mock multiple platforms for testing."""
    return [mock_enos_platform, mock_raspberry_pi_platform]


@pytest.fixture
def mock_enoslib():
    """Mock enoslib module."""
    with patch('enoslib') as mock_en:
        # Mock common enoslib functions
        mock_en.init_logging.return_value = None
        mock_en.check.return_value = None
        mock_en.sync_info.return_value = {}

        # Mock provider classes
        mock_en.G5k.return_value = MagicMock()
        mock_en.VMonG5k.return_value = MagicMock()
        mock_en.Iotlab.return_value = MagicMock()
        mock_en.Providers.return_value = MagicMock()

        # Mock configuration classes
        mock_en.G5kConf.from_dictionary.return_value.finalize.return_value = {}
        mock_en.VMonG5kConf.from_dictionary.return_value.finalize.return_value = {}
        mock_en.IotlabConf.from_dictionary.return_value.finalize.return_value = {}

        yield mock_en


@pytest.fixture
def mock_yaml_load():
    """Mock yaml.safe_load function."""
    with patch('yaml.safe_load') as mock_load:
        yield mock_load


@pytest.fixture
def mock_subprocess():
    """Mock subprocess module."""
    with patch('subprocess') as mock_sub:
        mock_sub.check_output.return_value = b'{"items": []}'
        mock_sub.run.return_value.returncode = 0
        yield mock_sub


@pytest.fixture
def mock_file_operations():
    """Mock file operations."""
    with patch('builtins.open') as mock_open_func, \
            patch('os.path.exists') as mock_exists, \
            patch('os.makedirs') as mock_makedirs:
        mock_exists.return_value = True
        yield {
            'open': mock_open_func,
            'exists': mock_exists,
            'makedirs': mock_makedirs
        }


@pytest.fixture(autouse=True)
def reset_platform_factory():
    """Reset PlatformFactory state between tests."""
    # This fixture automatically runs before each test
    # to ensure clean state for platform factory tests
    yield
    # Any cleanup code would go here if needed


@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary configuration file for testing."""
    config_content = """
[scalehub]
inventory = /tmp/test_inventory

[platforms]
platforms = test_grid5000
enable_ipv6 = true

[platforms.test_grid5000]
type = Grid5000
name = test_grid5000
site = rennes
cluster = paradoxe
producers = 2
consumers = 3
"""
    config_file = tmp_path / "test_config.ini"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def temp_inventory_file(tmp_path):
    """Create a temporary inventory file for testing."""
    inventory_content = """
pico:
  hosts:
    pi1:
      ansible_ssh_host: 192.168.1.1
      ansible_user: pi
      ansible_ssh_private_key_file: ~/.ssh/id_rsa
    pi2:
      ansible_ssh_host: 192.168.1.2
      ansible_user: pi
      ansible_ssh_private_key_file: ~/.ssh/id_rsa
"""
    inventory_file = tmp_path / "test_inventory.yaml"
    inventory_file.write_text(inventory_content)
    return str(inventory_file)