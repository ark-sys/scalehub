import pytest
from unittest.mock import patch, MagicMock
from scripts.src.platforms.EnosPlatform import EnosPlatform, EnosConfigurationError
from scripts.src.platforms.Platform import Platform


@pytest.fixture
def valid_config():
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
def logger_mock():
    return MagicMock()


def test_enosplatform_initialization(valid_config, logger_mock):
    """Test initialization of the EnosPlatform class."""
    platform = EnosPlatform(log=logger_mock, platform_config=valid_config)

    assert platform.platform_type == "Grid5000"
    # assert platform.platform_name == valid_config["reservation_name"]
    assert platform.platform_config == valid_config


def test_enosplatform_validate_config_success(valid_config, logger_mock):
    """Test successful validation of platform configuration."""
    platform = EnosPlatform(log=logger_mock, platform_config=valid_config)
    # No exception should be raised
    platform._validate_config()


def test_enosplatform_validate_config_failure(logger_mock):
    """Test validation failure when required fields are missing."""
    invalid_config = {"type": "Grid5000", "reservation_name": "test_reservation"}
    with pytest.raises(EnosConfigurationError, match="Missing required field: walltime"):
        EnosPlatform(log=logger_mock, platform_config=invalid_config)


def test_enosplatform_create_base_config(valid_config, logger_mock):
    """Test creation of the base configuration."""
    platform = EnosPlatform(log=logger_mock, platform_config=valid_config)
    base_config = platform._create_base_config()

    assert base_config["job_name"] == valid_config["reservation_name"]
    assert base_config["walltime"] == valid_config["walltime"]
    assert base_config["queue"] == valid_config["queue"]


@patch("scripts.src.platforms.EnosPlatform.subprocess.check_output")
def test_enosplatform_estimate_required_nodes(mock_subprocess, valid_config, logger_mock):
    """Test estimation of required nodes for VM groups."""
    mock_subprocess.return_value = b'{"items": [{"architecture": {"nb_cores": 16}, "main_memory": {"ram_size": 128000}}]}'
    platform = EnosPlatform(log=logger_mock, platform_config=valid_config)

    vm_groups = [
        {"role": "producers", "conf": {"core_per_vm": 4, "memory_per_vm": 8192}, "count": 2},
        {"role": "consumers", "conf": {"core_per_vm": 2, "memory_per_vm": 4096}, "count": 3},
    ]
    platform._estimate_required_nodes(vm_groups, "rennes", "paradoxe")

    assert vm_groups[0]["required_nodes"] == 1
    assert vm_groups[1]["required_nodes"] == 1


@patch("scripts.src.platforms.EnosPlatform.en.init_logging")
@patch("scripts.src.platforms.EnosPlatform.EnosPlatform._create_base_config")
def test_enosplatform_setup(mock_create_base_config, mock_init_logging, valid_config, logger_mock):
    """Test the setup method of the EnosPlatform class."""
    mock_create_base_config.return_value = {"job_name": "test_job"}
    platform = EnosPlatform(log=logger_mock, platform_config=valid_config)

    with patch.object(platform, "_get_config_dict", return_value={"config": "test"}):
        result = platform.setup()

    assert result == {"config": "test"}
    mock_init_logging.assert_called_once()