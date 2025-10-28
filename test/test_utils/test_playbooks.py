from unittest.mock import patch

import pytest

from src.utils.Config import Config
from src.utils.Logger import Logger
from utils.Playbooks import Playbooks


class TestPlaybooks:
    """Test suite for the Playbooks class."""

    @pytest.fixture
    def logger(self):
        """Fixture for a Logger instance."""
        return Logger()

    @pytest.fixture
    def config(self, logger):
        """Fixture for a Config instance."""
        return Config(
            logger,
            {
                "scalehub.inventory": "/path/to/inventory",
                "scalehub.playbook": "/path/to/playbooks",
            },
        )

    @pytest.fixture
    def playbooks(self, logger):
        """Fixture for a Playbooks instance."""
        return Playbooks(logger)

    def test_run_playbook_success(self, playbooks, config):
        """Test running a playbook successfully."""
        with patch("os.path.exists", return_value=True), patch("ansible_runner.run") as mock_run:
            mock_run.return_value.rc = 0
            playbooks.run("test_playbook", config)
            mock_run.assert_called_once()

    def test_run_playbook_file_not_found(self, playbooks, config):
        """Test running a playbook with a missing file."""
        with patch("os.path.exists", side_effect=[False, True]):
            with pytest.raises(
                FileNotFoundError,
                match="The file doesn't exist: /path/to/playbooks/test_playbook.yaml",
            ):
                playbooks.run("test_playbook", config)

    def test_reload_playbook(self, playbooks, config):
        """Test reloading a playbook."""
        with patch.object(playbooks, "run") as mock_run, patch.object(
            playbooks, "role_load_generators"
        ) as mock_role:
            playbooks.reload_playbook("test_playbook", config)
            assert mock_run.call_count == 2 or mock_role.call_count == 2

    def test_role_load_generators(self, playbooks, config):
        """Test handling load generator roles."""
        config_data = {
            "experiment.generators": [
                {
                    "name": "gen1",
                    "type": "type1",
                    "topic": "topic1",
                    "num_sensors": 10,
                    "interval_ms": 1000,
                    "replicas": 2,
                    "value": 42,
                }
            ]
        }
        config = Config(playbooks._Playbooks__log, config_data)
        with patch.object(playbooks, "run") as mock_run:
            playbooks.role_load_generators(config)
            mock_run.assert_called_once_with(
                "application/load_generators",
                config=config,
                tag=None,
                extra_vars={
                    "lg_name": "gen1",
                    "lg_topic": "topic1",
                    "lg_type": "type1",
                    "lg_numsensors": 10,
                    "lg_intervalms": 1000,
                    "lg_replicas": 2,
                    "lg_value": 42,
                },
                quiet=True,
            )
