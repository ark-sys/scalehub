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

import os

from scripts.src.data.processing.factory import ProcessorFactory
from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class DataManager:
    def __init__(self, logger: Logger, config: Config):
        self._logger = logger
        self._config = config
        self._processor_factory = ProcessorFactory(logger, config)

    def export(
        self,
        exp_path: str,
    ):
        # Retrieve base experiments directory from config if exp_path is not absolute
        if not os.path.isabs(exp_path):
            base_dir = self._config.get_str("scalehub.experiments")
            exp_path = os.path.join(base_dir, exp_path)
        processor = self._processor_factory.create_processor(exp_path)
        processor.process()
