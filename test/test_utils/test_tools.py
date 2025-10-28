from unittest.mock import patch, mock_open, MagicMock

import pytest

from src.utils.Logger import Logger
from src.utils.Tools import FolderManager, Tools


class TestFolderManager:
    """Test suite for the FolderManager class."""

    @pytest.fixture
    def logger(self):
        """Fixture for a Logger instance."""
        return Logger()

    @pytest.fixture
    def folder_manager(self, logger):
        """Fixture for a FolderManager instance."""
        return FolderManager(logger, "/base/path")

    def test_create_date_folder(self, folder_manager):
        """Test creating a date folder."""
        with patch("os.makedirs") as mock_makedirs:
            folder_manager.create_date_folder()
            mock_makedirs.assert_called_once_with(folder_manager.date_path)

    def test_create_subfolder_single_run(self, folder_manager):
        """Test creating a single run subfolder."""
        with patch("os.listdir", return_value=["1", "2"]), patch(
            "os.path.isdir", return_value=True
        ), patch("os.makedirs") as mock_makedirs:
            result = folder_manager.create_subfolder("/base/path", "single_run")
            assert result == "/base/path/3"
            mock_makedirs.assert_called_once_with("/base/path/3", exist_ok=True)

    def test_create_subfolder_res_exp(self, folder_manager):
        """Test creating a res_exp subfolder."""
        with patch("os.listdir", return_value=["res_exp_node1_1"]), patch(
            "os.makedirs"
        ) as mock_makedirs:
            result = folder_manager.create_subfolder("/base/path", "res_exp", node_name="node1")
            assert result == "/base/path/res_exp_node1_2"
            mock_makedirs.assert_called_once_with("/base/path/res_exp_node1_2", exist_ok=True)

    def test_create_subfolder_invalid_type(self, folder_manager):
        """Test creating a subfolder with an invalid type."""
        with patch("os.listdir", return_value=[]):
            result = folder_manager.create_subfolder("/base/path", "invalid")
            assert result is None


class TestTools:
    """Test suite for the Tools class."""

    @pytest.fixture
    def logger(self):
        """Fixture for a Logger instance."""
        return Logger()

    @pytest.fixture
    def tools(self, logger):
        """Fixture for a Tools instance."""
        return Tools(logger)

    def test_sync_data(self, tools):
        """Test syncing data using rsync."""
        with patch("subprocess.run") as mock_run:
            tools.sync_data("/experiments/path")
            mock_run.assert_called_once_with(
                "rsync -avz --ignore-existing rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc/ /experiments/path",
                shell=True,
            )

    def test_generate_grafana_quicklink(self, tools):
        """Test generating Grafana quicklinks."""
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = [
                {"title": "Scalehub monitoring", "url": "/dashboard"}
            ]
            mock_get.return_value = mock_response

            quicklink_local, quicklink_remote = tools.generate_grafana_quicklink(1000, 2000)
            assert (
                quicklink_local
                == "http://localhost/http://grafana.monitoring.svc.cluster.local/dashboard?from=1000000&to=2000000"
            )
            assert (
                quicklink_remote
                == "http://grafana.scalehub.dev/http://grafana.monitoring.svc.cluster.local/dashboard?from=1000000&to=2000000"
            )

    def test_create_log_file(self, tools):
        """Test creating a log file."""
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = [
                {"title": "Scalehub monitoring", "url": "/dashboard"}
            ]
            mock_get.return_value = mock_response

            with patch("builtins.open", mock_open()) as mock_file:
                with patch("json.dumps", return_value="{}"):
                    tools.create_log_file({}, "/exp/path", 1000, 2000)
                    mock_file.assert_called_once_with("/exp/path/exp_log.json", "w")
                    mock_file().write.assert_called_once_with("{}")

    def test_load_resource_definition(self, tools):
        """Test loading a resource definition."""
        resource_content = "key: {{ value }}"
        rendered_content = "key: test_value"
        with patch("builtins.open", mock_open(read_data=resource_content)), patch(
            "jinja2.Template.render", return_value=rendered_content
        ), patch("yaml.safe_load", return_value={"key": "test_value"}):
            result = tools.load_resource_definition("resource.yaml", {"value": "test_value"})
            assert result == {"key": "test_value"}
