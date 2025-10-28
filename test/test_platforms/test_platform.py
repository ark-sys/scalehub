import pytest

from src.scalehub.platforms.Platform import Platform


class MockPlatform(Platform):
    """Mock implementation of the abstract Platform class for testing."""

    def _validate_config(self) -> None:
        if "mock_key" not in self._platform_config:
            raise ValueError("Missing 'mock_key' in configuration")

    def setup(self, verbose: bool = False):
        return {"status": "setup complete"}

    def destroy(self):
        pass


def test_platform_initialization():
    """Test initialization of the Platform class."""
    config = {"type": "mock", "name": "mock_platform", "mock_key": "value"}
    platform = MockPlatform(log=None, platform_config=config)

    assert platform.platform_type == "mock"
    assert platform.platform_name == "mock_platform"
    assert platform.platform_config == config


def test_platform_missing_config_key():
    """Test validation failure when required config key is missing."""
    config = {"type": "mock", "name": "mock_platform"}
    with pytest.raises(ValueError, match="Missing 'mock_key' in configuration"):
        MockPlatform(log=None, platform_config=config)


def test_platform_setup():
    """Test the setup method of the Platform class."""
    config = {"type": "mock", "name": "mock_platform", "mock_key": "value"}
    platform = MockPlatform(log=None, platform_config=config)

    result = platform.setup()
    assert result == {"status": "setup complete"}


def test_platform_destroy():
    """Test the destroy method of the Platform class."""
    config = {"type": "mock", "name": "mock_platform", "mock_key": "value"}
    platform = MockPlatform(log=None, platform_config=config)

    # Ensure destroy does not raise any exceptions
    platform.destroy()
