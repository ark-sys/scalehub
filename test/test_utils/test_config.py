import json
from unittest.mock import patch, mock_open

import pytest

from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class TestConfig:
    """Test suite for the Config class."""

    @pytest.fixture
    def logger(self):
        """Fixture for a Logger instance."""
        return Logger()

    @pytest.fixture
    def config_dict(self):
        """Fixture for a sample configuration dictionary."""
        return {"key1": "value1", "key2": 42, "key3": True}

    def test_initialization_with_dict(self, logger, config_dict):
        """Test Config initializes correctly with a dictionary."""
        config = Config(logger, config_dict)
        assert str(config) == f"Config: {config_dict}"

    def test_initialization_with_invalid_type(self, logger):
        """Test Config raises ValueError for invalid initialization type."""
        with pytest.raises(ValueError, match="Invalid type for conf:"):
            Config(logger, 123)

    def test_load_from_nonexistent_file(self, logger):
        """Test Config raises FileNotFoundError for nonexistent file."""
        with pytest.raises(FileNotFoundError, match="Configuration file .* not found."):
            Config(logger, "nonexistent_file.ini")

    def test_load_from_invalid_file_type(self, logger):
        """Test Config raises ValueError for unsupported file type."""
        with patch("os.path.exists", return_value=True):
            with pytest.raises(ValueError, match="Invalid configuration file .*"):
                Config(logger, "invalid_file.txt")

    def test_load_from_json_file(self, logger, config_dict):
        """Test Config loads correctly from a JSON file."""
        json_content = json.dumps(config_dict)
        with patch("builtins.open", mock_open(read_data=json_content)):
            with patch("os.path.exists", return_value=True):
                config = Config(logger, "config.json")
                assert config.get("key1") == "value1"
                assert config.get_int("key2") == 42
                assert config.get_bool("key3") is True

    def test_load_from_ini_file(self, logger):
        """Test Config loads correctly from an INI file."""
        # Mock the defaults file content with all required sections
        defaults_content = """[scalehub]
inventory = default_inventory
playbook = default_playbook
experiments = default_experiments
debug_level = 0

[experiment]
name = default_name
job_file = default_job
task_name = default_task
output_skip_s = 0
output_stats = false
output_plot = false
broker_mqtt_host = localhost
broker_mqtt_port = 1883
kafka_partitions = 1
unchained_tasks = false
type = default
runs = 1

[experiment.flink]
checkpoint_interval_ms = 5000
window_size_ms = 10000
fibonacci_value = 20

[experiment.generators]
generators = gen1

[experiment.generators.gen1]
type = sensor
topic = test_topic
num_sensors = 10
interval_ms = 1000
replicas = 1
value = 42
"""

        # Test file content
        ini_content = """[scalehub]
inventory = inventory_value
debug_level = 1

[experiment]
name = test_experiment
runs = 5
"""

        def mock_open_side_effect(filename, *args, **kwargs):
            if filename == "/app/conf/defaults.ini":
                return mock_open(read_data=defaults_content).return_value
            else:
                return mock_open(read_data=ini_content).return_value

        with patch("builtins.open", side_effect=mock_open_side_effect):
            with patch("os.path.exists", return_value=True):
                config = Config(logger, "config.ini")
                assert config.get("scalehub.inventory") == "inventory_value"
                assert config.get_int("experiment.runs") == 5
                assert config.get_str("experiment.name") == "test_experiment"

    def test_get_methods(self, logger, config_dict):
        """Test utility methods for retrieving configuration values."""
        config = Config(logger, config_dict)
        assert config.get("key1") == "value1"
        assert config.get_int("key2") == 42
        assert config.get_bool("key3") is True
        assert config.get_str("key1") == "value1"

    def test_update_runtime_file(self, logger, config_dict):
        """Test updating the runtime file."""
        config = Config(logger, config_dict)

        with patch("os.path.exists", return_value=True):
            with patch("json.load", return_value={"existing_key": "existing_value"}):
                with patch("json.dump") as mock_dump:
                    with patch("builtins.open", mock_open()) as mock_file:
                        config.update_runtime_file()
                        # Verify json.dump was called with merged data
                        mock_dump.assert_called_once()
                        dumped_data = mock_dump.call_args[0][0]
                        assert "existing_key" in dumped_data
                        assert dumped_data["key1"] == "value1"

    def test_delete_runtime_file(self, logger):
        """Test deleting the runtime file."""
        config = Config(logger, {})
        with patch("os.path.exists", return_value=True):
            with patch("os.remove") as mock_remove:
                config.delete_runtime_file()
                mock_remove.assert_called_once_with(Config.RUNTIME_PATH)

    def test_to_json(self, logger, config_dict):
        """Test converting the configuration to JSON."""
        config = Config(logger, config_dict)
        assert config.to_json() == json.dumps(config_dict, indent=4)
