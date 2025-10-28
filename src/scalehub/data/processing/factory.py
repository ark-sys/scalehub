# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import re
from pathlib import Path

from src.utils.Config import Config
from src.utils.Logger import Logger


class ProcessorFactory:
    """Factory class for creating appropriate data processing based on experiment type."""

    def __init__(self, logger: Logger, config: Config):
        self._logger = logger
        self._config = config

    def create_processor(self, exp_path: str):
        path = Path(exp_path)
        folder_type = self._determine_folder_type(path)

        """Create appropriate processor based on experiment path structure."""
        if folder_type in ["single_run"]:
            from src.scalehub.data.processing.single_experiment_processor import (
                SingleExperimentProcessor,
            )

            return SingleExperimentProcessor(self._logger, self._config, exp_path)
        elif folder_type in ["multi_run", "res_exp", "multi_exp", "date"]:
            # Date folders and multi-run/multi-exp folders are all grouped experiments
            from src.scalehub.data.processing.grouped_experiment_processor import (
                GroupedExperimentProcessor,
            )

            return GroupedExperimentProcessor(self._logger, self._config, exp_path)
        else:
            raise ValueError(f"Unsupported folder type: {folder_type} for path: {exp_path}")

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
