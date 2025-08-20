from unittest.mock import MagicMock, patch

import pytest

from scripts.src.platforms.EnosPlatform import EnosPlatform
from scripts.src.platforms.EnosPlatforms import EnosPlatforms


@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def enos_platform_mock():
    platform = MagicMock(spec=EnosPlatform)
    platform.platform_type = "Grid5000"
    platform.start_time = "now"
    platform.platform_config = {"name": "test_platform"}
    platform.setup.return_value = {
        "resources": {"machines": [{"roles": ["producers"], "nodes": 2}]}
    }
    platform.get_provider.return_value = MagicMock()
    return platform


def test_build_uber_dict(logger_mock, enos_platform_mock):
    """Test building uber configuration dictionary."""
    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)

    result = enos_platforms.uber_dict

    assert "Grid5000" in result
    assert result["Grid5000"]["resources"]["machines"][0]["roles"] == ["producers"]
    enos_platform_mock.setup.assert_called_once()


def test_build_uber_dict_merge_configs(logger_mock):
    """Test merging configurations for same platform type."""
    platform1 = MagicMock(spec=EnosPlatform)
    platform1.platform_type = "Grid5000"
    platform1.setup.return_value = {
        "resources": {"machines": [{"roles": ["producers"], "nodes": 2}]}
    }

    platform2 = MagicMock(spec=EnosPlatform)
    platform2.platform_type = "Grid5000"
    platform2.setup.return_value = {
        "resources": {"machines": [{"roles": ["consumers"], "nodes": 3}]}
    }

    platforms = [platform1, platform2]
    enos_platforms = EnosPlatforms(logger_mock, platforms)

    result = enos_platforms.uber_dict

    assert len(result["Grid5000"]["resources"]["machines"]) == 2


@patch("scripts.src.platforms.EnosPlatforms.en.Providers")
def test_setup_success(mock_providers, logger_mock, enos_platform_mock):
    """Test successful setup of EnosPlatforms."""
    # Mock the providers and their methods
    mock_provider_instance = MagicMock()
    mock_providers.return_value = mock_provider_instance

    mock_roles = {
        "G5k": [
            MagicMock(alias="node1", address="192.168.1.1"),
            MagicMock(alias="node2", address="192.168.1.2"),
        ]
    }
    mock_networks = []

    mock_provider_instance.init.return_value = (mock_roles, mock_networks)

    with patch(
        "scripts.src.platforms.EnosPlatforms.en.sync_info", return_value=mock_roles
    ):
        platforms = [enos_platform_mock]
        enos_platforms = EnosPlatforms(logger_mock, platforms)
        inventory = enos_platforms.setup()

    assert inventory is not None
    assert "G5k" in inventory
    mock_provider_instance.init.assert_called_once()


@pytest.fixture
def vagrant_platform_mock():
    from scripts.src.platforms.EnosPlatform import VMGroup

    platform = MagicMock(spec=EnosPlatform)
    platform.platform_type = "VagrantG5k"
    platform.start_time = "now"
    platform.platform_config = {"name": "vagrant_platform"}
    platform.vm_groups = [
        VMGroup(
            role="producers",
            conf={
                "core_per_vm": 4,
                "memory_per_vm": 8192,
                "disk_per_vm": 100,
                "site": "rennes",
                "cluster": "paradoxe",
            },
            count=2,
            required_nodes=1,
        )
    ]
    platform.setup.return_value = {
        "resources": {"machines": [{"roles": ["vagrant"], "nodes": 1}]}
    }
    platform.get_provider.return_value = MagicMock()
    return platform


def test_reformat_inventory(logger_mock, enos_platform_mock):
    """Test inventory reformatting."""
    inventory = {
        "test_platform": {"hosts": {"node1": {"ansible_host": "192.168.1.1"}}},
        "producers": {"hosts": {"node1": {"ansible_host": "192.168.1.1"}}},
        "baremetal": {"hosts": {"node1": {"ansible_host": "192.168.1.1"}}},
    }

    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)
    result = enos_platforms._EnosPlatforms__reformat_inventory(inventory)

    assert "test_platform" not in result
    assert "producers" not in result
    assert "baremetal" not in result
    assert "G5k" in result
    assert "agents" in result


def test_gen_reservation_name(logger_mock, enos_platform_mock):
    """Test reservation name generation."""
    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)

    # Test with "now" start time
    name = enos_platforms._EnosPlatforms__gen_reservation_name("Grid5000", "now")
    assert name == "scalehub_baremetal"

    # Test with specific time (day)
    name = enos_platforms._EnosPlatforms__gen_reservation_name("Grid5000", "10:30:00")
    assert name == "scalehub_baremetal_day"

    # Test with late time
    name = enos_platforms._EnosPlatforms__gen_reservation_name("VMonG5k", "20:00:00")
    assert name == "scalehub_virtualmachines_late"


def test_get_providers(logger_mock, enos_platform_mock):
    """Test provider creation."""
    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)

    enos_platform_mock.get_provider.reset_mock()

    conf_dict = {"Grid5000": {"job_name": "test_job", "resources": {}}}

    providers = enos_platforms._EnosPlatforms__get_providers(conf_dict)

    assert len(providers) == 1
    enos_platform_mock.get_provider.assert_called_once()


@patch("scripts.src.platforms.EnosPlatforms.isinstance")
@patch("scripts.src.platforms.EnosPlatforms.en.G5k")
def test_post_setup(mock_g5k, mock_isinstance, logger_mock, enos_platform_mock):
    """Test post-setup firewall configuration."""
    mock_provider = MagicMock()
    mock_provider.fw_create = MagicMock()

    # Mock isinstance to return True for our mock provider
    mock_isinstance.return_value = True

    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)
    enos_platforms.enos_providers = [mock_provider]

    enos_platforms.post_setup()

    mock_provider.fw_create.assert_called_once_with(proto="all")
    mock_isinstance.assert_called_once_with(mock_provider, mock_g5k)


def test_destroy(logger_mock, enos_platform_mock):
    """Test platform destruction."""
    mock_provider = MagicMock()

    platforms = [enos_platform_mock]
    enos_platforms = EnosPlatforms(logger_mock, platforms)
    enos_platforms.enos_providers = [mock_provider]

    enos_platforms.destroy()

    mock_provider.destroy.assert_called_once()
    logger_mock.info.assert_called_with("[ENOS_PLTS] Enos platforms destroyed.")


def test_setup_with_start_time(logger_mock):
    """Test setup with custom start time."""
    platform = MagicMock(spec=EnosPlatform)
    platform.platform_type = "Grid5000"
    platform.start_time = "10:30:00"
    platform.setup.return_value = {"resources": {"machines": []}}
    platform.get_provider.return_value = MagicMock()

    with patch("scripts.src.platforms.EnosPlatforms.en.Providers") as mock_providers:
        mock_provider_instance = MagicMock()
        mock_providers.return_value = mock_provider_instance
        mock_provider_instance.init.return_value = ({}, [])

        with patch("scripts.src.platforms.EnosPlatforms.en.sync_info", return_value={}):
            platforms = [platform]
            enos_platforms = EnosPlatforms(logger_mock, platforms)
            enos_platforms.setup()

        # Verify that init was called with a timestamp
        mock_provider_instance.init.assert_called_once()
        call_args = mock_provider_instance.init.call_args
        assert call_args[1]["start_time"] is not None
