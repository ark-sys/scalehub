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

from time import sleep

from src.monitor.experiments.Experiment import Experiment


class TestExperiment(Experiment):
    exp_type = "test"

    def __init__(self, log, config):
        super().__init__(log, config)
        self.__log = log

    def starting(self):
        self.__log.info("[TEST_E] Starting experiment.")
        # Create a new thread for the experiment
        self.start_thread(self._do_some_running)

    def finishing(self):
        self.__log.info("[TEST_E] Finishing experiment.")
        sleep(10)
        # Export data here

    def cleaning(self):
        self.__log.info("[TEST_E] Cleaning experiment.")
        sleep(10)
        # Clean up resources here

    def running(self):
        self.__log.info("[TEST_E] Running experiment.")
        # Wait for the thread to finish
        self.join_thread()

    def _do_some_running(self):
        self.__log.info("[TEST_E] Doing some running.")
        sleep_time = 60
        for i in range(sleep_time):
            if self.current_experiment_thread.__stopped():
                self.__log.info("[TEST_E] Stopped running.")
                break
            sleep(1)
        self.__log.info("[TEST_E] Finished running.")
