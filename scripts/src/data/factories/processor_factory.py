import re
from pathlib import Path

from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class ProcessorFactory:
    """Factory class for creating appropriate data processors based on experiment type."""

    def __init__(self, logger: Logger, config: Config):
        self._logger = logger
        self._config = config

    def create_processor(self, exp_path: str):
        path = Path(exp_path)
        folder_type = self._determine_folder_type(path)

        """Create appropriate processor based on experiment path structure."""
        if folder_type in ["single_run"]:
            from scripts.src.data.processors.single_experiment_processor import (
                SingleExperimentProcessor,
            )
            return SingleExperimentProcessor(self._logger, self._config, exp_path)
        elif folder_type in ["multi_run", "res_exp", "multi_exp"]:
            from scripts.src.data.processors.grouped_experiment_processor import (
                GroupedExperimentProcessor,
            )
            return GroupedExperimentProcessor(self._logger, self._config, exp_path)
        else:
            raise ValueError(
                f"Unsupported folder type: {folder_type} for path: {exp_path}"
            )

    def _determine_folder_type(self, path: Path) -> str:
        """Determine the type of experiment folder."""
        basename = path.name

        self._logger.debug(f"Determining folder type for: {basename}")

        if re.match(r"^\d{4}-\d{2}-\d{2}$", basename):
            return "date"
        elif re.match(r"^\d+$", basename):
            return "single_run"
        elif re.match(r"^multi_run_\d+$", basename) or any(
            re.match(r"^\d+$", f.name) for f in path.iterdir() if f.is_dir()
        ):
            return "multi_run"
        elif re.match(r"^res_exp_\w+(_\d+)?$", basename):
            return "res_exp"
        elif re.match(r"^multi_exp_\d+(_\d+[a-zA-Z])?$", basename):
            return "multi_exp"
        else:
            return "unknown"
