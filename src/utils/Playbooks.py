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
from time import sleep

import ansible_runner

from src.utils.Config import Config
from src.utils.Defaults import DefaultKeys as Key
from src.utils.Logger import Logger


class Playbooks:
    def __init__(self, log: Logger):
        self.__log = log

    def role_load_generators(self, config: Config, tag=None):
        for lg_conf in config.get(Key.Experiment.Generators.generators.key):
            try:
                lg_params = {
                    "lg_name": lg_conf["name"],
                    "lg_topic": lg_conf["topic"],
                    "lg_type": lg_conf["type"],
                    "lg_numsensors": int(lg_conf["num_sensors"]),
                    "lg_intervalms": int(lg_conf["interval_ms"]),
                    "lg_replicas": int(lg_conf["replicas"]),
                    "lg_value": int(lg_conf["value"]),
                }
                self.run(
                    "application/load_generators",
                    config=config,
                    tag=tag,
                    extra_vars=lg_params,
                    quiet=True,
                )
            except Exception as e:
                self.__log.error(str(e))
                raise e

    def reload_playbook(self, playbook, config: Config, extra_vars=None):
        self.__log.info(f"Reloading playbook: {playbook}")
        try:
            if "load_generators" in playbook:
                self.role_load_generators(config, tag="delete")
            else:
                self.run(
                    playbook,
                    config=config,
                    tag="delete",
                    quiet=True,
                    extra_vars=extra_vars,
                )
            sleep(5)
            if "load_generators" in playbook:
                self.role_load_generators(config, tag="create")
            else:
                self.run(
                    playbook,
                    config=config,
                    tag="create",
                    quiet=True,
                    extra_vars=extra_vars,
                )
        except Exception as e:
            self.__log.error(str(e))
            raise e

    def run(self, playbook, config: Config, tag=None, extra_vars=None, quiet=False):
        if extra_vars is None:
            extra_vars = {}
        inventory = config.get_str(Key.Scalehub.inventory.key)
        playbook_filename = f"{config.get_str(Key.Scalehub.playbook.key)}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"[PLAY] The file doesn't exist: {playbook_filename}")
        if not os.path.exists(inventory):
            # This can happen when running in experiment-monitor. Just create a dummy inventory file with localhost
            inventory = "/tmp/inventory"
            with open(inventory, "w") as f:
                f.write("localhost ansible_connection=local")

        playbook_vars = {
            "shub_config": config.to_json(),
        }
        playbook_vars.update(extra_vars)

        tags = tag if tag else ""

        self.__log.debug(f"[PLAY] Running playbook: {playbook_filename}, tags: {tags}")
        self.__log.debug(f"[PLAY] Inventory: {inventory}")
        self.__log.debug(f"[PLAY] Extra vars: {playbook_vars}")
        # Retrieve debug level from config
        debug_level = (
            config.get_int(Key.Scalehub.debug_level.key)
            if config.get_int(Key.Scalehub.debug_level.key) is not None
            else 0
        )

        # Run the playbook with additional tags and extra vars
        try:
            r = ansible_runner.run(
                private_data_dir="/tmp/ansible",
                playbook=playbook_filename,
                inventory=inventory,
                extravars=playbook_vars,
                tags=tags,
                quiet=quiet,
                verbosity=debug_level,
            )
            if r.rc != 0:
                self.__log.error(f"[PLAY] Failed to run playbook: {playbook_filename}: {r.status}")
                self.__log.error(r.stdout.read())
                return
            else:
                self.__log.info(
                    f"[PLAY] Playbook {playbook_filename} with tag {tags} executed successfully."
                )
        except Exception as e:
            self.__log.error(e.__str__())
            raise e
