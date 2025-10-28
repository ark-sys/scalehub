from unittest.mock import Mock, patch

import pytest

from src.scalehub.resources.FlinkManager import FlinkManager
from src.scalehub.resources.KubernetesManager import KubernetesManager
from src.utils.Config import Config
from src.utils.Logger import Logger


class TestFlinkManager:
    """Test suite for the FlinkManager class."""

    @pytest.fixture
    def logger(self):
        """Fixture for a Logger instance."""
        return Mock(spec=Logger)

    @pytest.fixture
    def config(self):
        """Fixture for a Config instance."""
        mock_config = Mock(spec=Config)
        mock_config.get_str.side_effect = lambda key: {
            "task_name": "test_task",
            "job_file": "test_job.jar",
        }.get(key.split(".")[-1], "default_value")
        return mock_config

    @pytest.fixture
    def kubernetes_manager(self, logger):
        """Fixture for a KubernetesManager instance."""
        mock_km = Mock(spec=KubernetesManager)
        mock_km.pod_manager = Mock()
        return mock_km

    @pytest.fixture
    def flink_manager(self, logger, config, kubernetes_manager):
        """Fixture for a FlinkManager instance."""
        return FlinkManager(logger, config, kubernetes_manager)

    @patch("requests.get")
    def test_get_overview_success(self, mock_get, flink_manager):
        """Test successful overview retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"slots-total": 10, "taskmanagers": 2}
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_overview()

        assert result == {"slots-total": 10, "taskmanagers": 2}
        mock_get.assert_called_once_with(
            "http://flink-jobmanager.flink.svc.cluster.local:8081/overview"
        )

    @patch("requests.get")
    def test_get_overview_failure(self, mock_get, flink_manager):
        """Test overview retrieval failure."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_overview()

        assert result is None

    @patch("requests.get")
    def test_get_overview_exception(self, mock_get, flink_manager):
        """Test overview retrieval with exception."""
        mock_get.side_effect = Exception("Connection failed")

        result = flink_manager._FlinkManager__get_overview()

        assert result is None
        flink_manager._FlinkManager__log.error.assert_called_with(
            "[FLK_MGR] Error while getting overview: Connection failed"
        )

    @patch("time.sleep")
    @patch("requests.get")
    def test_get_job_plan_success(self, mock_get, mock_sleep, flink_manager):
        """Test successful job plan retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"plan": {"nodes": []}}
        mock_response.text = '{"plan": {"nodes": []}}'
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_job_plan("test_job_id")

        assert result == {"plan": {"nodes": []}}
        mock_get.assert_called_once_with(
            "http://flink-jobmanager.flink.svc.cluster.local:8081/jobs/test_job_id/plan"
        )

    @patch("time.sleep")
    @patch("requests.get")
    def test_get_job_plan_retry_failure(self, mock_get, mock_sleep, flink_manager):
        """Test job plan retrieval with retries."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_job_plan("test_job_id")

        assert result is None
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 3

    @patch("requests.get")
    def test_get_job_state_success(self, mock_get, flink_manager):
        """Test successful job state retrieval."""
        flink_manager.job_id = "test_job_id"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "RUNNING"}
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_job_state()

        assert result == "RUNNING"

    @patch("requests.get")
    def test_get_job_state_failure(self, mock_get, flink_manager):
        """Test job state retrieval failure."""
        flink_manager.job_id = "test_job_id"
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = flink_manager._FlinkManager__get_job_state()

        assert result is None

    def test_get_operators_success(self, flink_manager):
        """Test successful operator extraction."""
        flink_manager.job_plan = {
            "plan": {
                "nodes": [
                    {"description": "Source: test</br>task", "parallelism": 2},
                    {"description": "Sink: output<br/>task", "parallelism": 4},
                ]
            }
        }

        result = flink_manager._FlinkManager__get_operators()

        expected = {"Source_test_task": 2, "Sink_output_task": 4}
        assert result == expected

    def test_get_operators_exception(self, flink_manager):
        """Test operator extraction with exception."""
        flink_manager.job_plan = None

        result = flink_manager._FlinkManager__get_operators()

        assert result is None

    def test_get_monitored_task_parallelism_found(self, flink_manager):
        """Test finding monitored task parallelism."""
        flink_manager.monitored_task = "test_task"
        flink_manager.operators = {"source_test_task": 2, "sink_other": 4}

        result = flink_manager._FlinkManager__get_monitored_task_parallelism()

        assert result == 2

    def test_get_monitored_task_parallelism_not_found(self, flink_manager):
        """Test monitored task parallelism not found."""
        flink_manager.monitored_task = "missing_task"
        flink_manager.operators = {"source_test_task": 2, "sink_other": 4}

        result = flink_manager._FlinkManager__get_monitored_task_parallelism()

        assert result is None

    @patch("time.sleep")
    def test_stop_job_success(self, mock_sleep, flink_manager):
        """Test successful job stop with savepoint."""
        flink_manager.job_id = "test_job_id"
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = (
            "Savepoint completed. Path: /tmp/savepoint123"
        )

        with patch.object(flink_manager, "_FlinkManager__get_job_state", return_value="RUNNING"):
            result = flink_manager._FlinkManager__stop_job()

        assert result == "/tmp/savepoint123"
        flink_manager.k.pod_manager.execute_command_on_pod.assert_called_with(
            deployment_name="flink-jobmanager", command="flink stop -p -d test_job_id"
        )

    @patch("time.sleep")
    def test_stop_job_failed_state(self, mock_sleep, flink_manager):
        """Test job stop when job is in failed state."""
        flink_manager.job_id = "test_job_id"

        with patch.object(flink_manager, "_FlinkManager__get_job_state", return_value="FAILED"):
            result = flink_manager._FlinkManager__stop_job()

        assert result is None

    @patch("time.sleep")
    def test_stop_job_no_savepoint(self, mock_sleep, flink_manager):
        """Test job stop when savepoint fails."""
        flink_manager.job_id = "test_job_id"
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = "No savepoint info"

        with patch.object(flink_manager, "_FlinkManager__get_job_state", return_value="RUNNING"):
            result = flink_manager._FlinkManager__stop_job()

        assert result is None

    def test_build_par_map(self, flink_manager):
        """Test building parallelism map."""
        flink_manager.monitored_task = "test_task"
        flink_manager.operators = {"source_test_task": 2, "sink_other": 4}

        result = flink_manager._FlinkManager__build_par_map(6)

        assert "source_test_task:6" in result
        assert "sink_other:4" in result

    def test_run_job_simple(self, flink_manager):
        """Test simple job run without parameters."""
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = "JobID abc123def456"

        result = flink_manager.run_job()

        assert flink_manager.job_id == "abc123def456"
        flink_manager.k.pod_manager.execute_command_on_pod.assert_called_with(
            deployment_name="flink-jobmanager",
            command="flink run -d -j /tmp/jobs/test_job.jar",
        )

    def test_run_job_with_start_par(self, flink_manager):
        """Test job run with start parallelism."""
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = "JobID abc123def456"

        result = flink_manager.run_job(start_par=4)

        assert flink_manager.job_id == "abc123def456"
        flink_manager.k.pod_manager.execute_command_on_pod.assert_called_with(
            deployment_name="flink-jobmanager",
            command="flink run -d -j /tmp/jobs/test_job.jar --start_par 4",
        )

    def test_run_job_with_rescale(self, flink_manager):
        """Test job run with rescaling."""
        flink_manager.monitored_task = "test_task"
        flink_manager.operators = {"source_test_task": 2, "sink_other": 4}
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = "JobID abc123def456"

        with patch.object(flink_manager, "_FlinkManager__stop_job", return_value="/tmp/savepoint"):
            result = flink_manager.run_job(new_parallelism=6)

        assert flink_manager.job_id == "abc123def456"
        expected_command = "flink run -d -s /tmp/savepoint -j /tmp/jobs/test_job.jar --parmap 'source_test_task:6;sink_other:4'"
        flink_manager.k.pod_manager.execute_command_on_pod.assert_called_with(
            deployment_name="flink-jobmanager", command=expected_command
        )

    def test_run_job_no_job_id_found(self, flink_manager):
        """Test job run when job ID extraction fails."""
        flink_manager.k.pod_manager.execute_command_on_pod.return_value = "No job ID in response"

        result = flink_manager.run_job()

        assert result == 1

    def test_get_total_slots(self, flink_manager):
        """Test getting total slots."""
        with patch.object(
            flink_manager,
            "_FlinkManager__get_overview",
            return_value={"slots-total": 10},
        ):
            result = flink_manager.get_total_slots()

        assert result == 10

    def test_get_total_taskmanagers(self, flink_manager):
        """Test getting total task managers."""
        with patch.object(
            flink_manager,
            "_FlinkManager__get_overview",
            return_value={"taskmanagers": 3},
        ):
            result = flink_manager.get_total_taskmanagers()

        assert result == 3

    def test_check_nominal_job_run(self, flink_manager):
        """Test checking nominal job run."""
        flink_manager.job_id = "current_job_123"
        flink_manager.k.pod_manager.execute_command_on_pod.side_effect = [
            "Job1 abc123def456\nJob2 xyz789uvw012\nJob3 current_job_123",
            "Job abc123def456 cancelled",
            "Job xyz789uvw012 cancelled",
        ]

        result = flink_manager.check_nominal_job_run()

        assert result == 0

    @patch("time.sleep")
    def test_wait_for_job_running_success(self, mock_sleep, flink_manager):
        """Test waiting for job to reach running state."""
        with patch.object(flink_manager, "_FlinkManager__get_job_state", return_value="RUNNING"):
            result = flink_manager.wait_for_job_running()

        assert result == 0

    @patch("time.sleep")
    def test_wait_for_job_running_timeout(self, mock_sleep, flink_manager):
        """Test waiting for job that never reaches running state."""
        with patch.object(flink_manager, "_FlinkManager__get_job_state", return_value="STARTING"):
            result = flink_manager.wait_for_job_running()

        assert result == 1
        assert mock_sleep.call_count == 15

    def test_get_job_info_success(self, flink_manager):
        """Test successful job info retrieval."""
        flink_manager.job_id = "test_job_id"

        with patch.object(
            flink_manager,
            "_FlinkManager__get_job_plan",
            return_value={"plan": {"nodes": []}},
        ), patch.object(
            flink_manager, "_FlinkManager__get_operators", return_value={"task": 2}
        ), patch.object(
            flink_manager,
            "_FlinkManager__get_monitored_task_parallelism",
            return_value=2,
        ):
            result = flink_manager.get_job_info()

        assert result == 0
        assert flink_manager.operators == {"task": 2}
        assert flink_manager.monitored_task_parallelism == 2

    def test_get_job_info_no_job_id(self, flink_manager):
        """Test job info retrieval without job ID."""
        flink_manager.job_id = None

        result = flink_manager.get_job_info()

        assert result is None

    def test_get_job_info_no_operators(self, flink_manager):
        """Test job info retrieval when operators not found."""
        flink_manager.job_id = "test_job_id"

        with patch.object(
            flink_manager,
            "_FlinkManager__get_job_plan",
            return_value={"plan": {"nodes": []}},
        ), patch.object(flink_manager, "_FlinkManager__get_operators", return_value=None):
            result = flink_manager.get_job_info()

        assert result is None
