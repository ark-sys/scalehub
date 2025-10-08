from scripts.utils.Defaults import ConfigKey, DefaultKeys


class TestConfigKey:
    """Test suite for the ConfigKey class."""

    def test_config_key_initialization(self):
        """Test ConfigKey initializes with correct attributes."""
        key = ConfigKey(
            "test.key", is_optional=True, default_value="default", extra="extra"
        )
        assert key.key == "test.key"
        assert key.is_optional is True
        assert key.default_value == "default"
        assert key.kwargs["extra"] == "extra"

    def test_config_key_default_values(self):
        """Test ConfigKey initializes with default values when not provided."""
        key = ConfigKey("test.key")
        assert key.key == "test.key"
        assert key.is_optional is False
        assert key.default_value is None

    def test_config_key_string_representation(self):
        """Test ConfigKey string representation returns the key."""
        key = ConfigKey("test.key")
        assert str(key) == "test.key"


class TestDefaultKeys:
    """Test suite for the DefaultKeys class."""

    def test_scalehub_keys(self):
        """Test Scalehub keys are defined correctly."""
        assert hasattr(DefaultKeys.Scalehub, "inventory")
        assert hasattr(DefaultKeys.Scalehub, "playbook")
        assert hasattr(DefaultKeys.Scalehub, "experiments")
        assert hasattr(DefaultKeys.Scalehub, "debug_level")
        assert hasattr(DefaultKeys.Scalehub, "lazy_setup")
        assert hasattr(DefaultKeys.Scalehub, "provision_infrastructure")
        assert hasattr(DefaultKeys.Scalehub, "k3s_version")

    def test_platforms_keys(self):
        """Test Platforms keys are defined correctly."""
        assert hasattr(DefaultKeys.Platforms, "platforms")
        assert hasattr(DefaultKeys.Platforms, "enable_ipv6")
        assert hasattr(DefaultKeys.Platforms, "tailscale_backed")
        assert hasattr(DefaultKeys.Platforms, "tailscale_auth_key")

    def test_experiment_keys(self):
        """Test Experiment keys are defined correctly."""
        assert hasattr(DefaultKeys.Experiment, "name")
        assert hasattr(DefaultKeys.Experiment, "job_file")
        assert hasattr(DefaultKeys.Experiment, "task_name")
        assert hasattr(DefaultKeys.Experiment, "output_skip_s")
        assert hasattr(DefaultKeys.Experiment, "output_stats")
        assert hasattr(DefaultKeys.Experiment, "output_plot")
        assert hasattr(DefaultKeys.Experiment, "broker_mqtt_host")
        assert hasattr(DefaultKeys.Experiment, "broker_mqtt_port")
        assert hasattr(DefaultKeys.Experiment, "kafka_partitions")
        assert hasattr(DefaultKeys.Experiment, "unchained_tasks")
        assert hasattr(DefaultKeys.Experiment, "type")
        assert hasattr(DefaultKeys.Experiment, "runs")
        assert hasattr(DefaultKeys.Experiment, "comment")

    def test_nested_keys(self):
        """Test nested keys in DefaultKeys are defined correctly."""
        assert hasattr(DefaultKeys.Experiment.Scaling, "strategy_path")
        assert hasattr(DefaultKeys.Experiment.Scaling, "interval_scaling_s")
        assert hasattr(DefaultKeys.Experiment.Scaling, "max_parallelism")
        assert hasattr(DefaultKeys.Experiment.Scaling, "steps")

        assert hasattr(DefaultKeys.Experiment.Generators, "generators")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "name")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "topic")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "type")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "num_sensors")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "interval_ms")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "replicas")
        assert hasattr(DefaultKeys.Experiment.Generators.Generator, "value")
