import os

import ansible_runner
import jinja2
import yaml

from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class Tools:
    ANSIBLE_CFG_PATH = "/app/conf/ansible.cfg"

    def __init__(self, log: Logger):
        self.__log = log

    def uncomment_ansible_cfg(self):
        # Check if we have "strategy_plugins" and "strategy" in the ansible.cfg file
        # If they are commented, uncomment them
        try:
            with open(self.ANSIBLE_CFG_PATH, "w") as f:
                lines = f.readlines()
                for line in lines:
                    if "strategy_plugins" in line:
                        f.write(line.replace("#", ""))
                    elif "strategy" in line:
                        f.write(line.replace("#", ""))
                    else:
                        f.write(line)
        except Exception as e:
            self.__log.error(f"Error while uncommenting ansible.cfg: {e}")
            raise e

    def comment_ansible_cfg(self):
        # Check if we have "strategy_plugins" and "strategy" in the ansible.cfg file
        # If they are not commented, comment them
        try:
            with open(self.ANSIBLE_CFG_PATH, "w") as f:
                lines = f.readlines()
                for line in lines:
                    if "strategy_plugins" in line and "#" not in line:
                        f.write("#" + line)
                    elif "strategy" in line and "#" not in line:
                        f.write("#" + line)
                    else:
                        continue
        except Exception as e:
            self.__log.error(f"Error while commenting ansible.cfg: {e}")
            raise e

    def create_exp_folder(self, base_path: str, date):
        # Create the base folder path
        base_folder_path = os.path.join(base_path, date)
        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1
        try:
            # Create the subfolder
            os.makedirs(subfolder_path)
        except OSError as e:
            self.__log.error(
                f"Error while creating experiment folder {subfolder_path}: {e}"
            )
            raise e

        self.__log.info(f"Created experiment folder: {subfolder_path}")
        # Return the path to the new subfolder
        return subfolder_path

    def get_timestamp_from_log(self, full_exp_path):
        # Get the log file
        log_file = os.path.join(full_exp_path, "log.txt")
        # Read the log file
        with open(log_file, "r") as file:
            lines = file.readlines()
        # Get the timestamp start and end timestamps
        start_ts = lines[0].split(":")[1].strip()
        end_ts = lines[-1].split(":")[1].strip()
        return start_ts, end_ts

    def load_resource_definition(self, resource_filename, experiment_params):
        try:
            with open(resource_filename, "r") as f:
                resource_template = f.read()
                resource_definition = jinja2.Template(resource_template).render(
                    experiment_params
                )
            resource_object = yaml.safe_load(resource_definition)
            return resource_object
        except FileNotFoundError as e:
            self.__log.error(f"File not found: {resource_filename}")
            return


class Playbooks:
    def __init__(self, log: Logger):
        self.__log = log

    def run(self, playbook, config: Config, tag=None, extra_vars=None):
        if extra_vars is None:
            extra_vars = {}
        inventory = config.get_str(Key.Scalehub.inventory)
        playbook_filename = f"{config.get_str(Key.Scalehub.playbook)}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")
        if not os.path.exists(inventory):
            # This can happen when running in experiment-monitor. Just create a dummy inventory file with localhost
            inventory = "localhost"
        playbook_vars = {
            "shub_config": config.to_json(),
        }
        playbook_vars.update(extra_vars)

        tags = tag if tag else ""

        # Run the playbook with additional tags and extra vars
        try:
            r = ansible_runner.run(
                private_data_dir="/tmp/ansible",
                playbook=playbook_filename,
                inventory=inventory,
                extravars=playbook_vars,
                tags=tags,
            )
            if r.rc != 0:
                self.__log.error(f"Failed to run playbook: {playbook_filename}")
                return r.rc
            else:
                self.__log.info(f"Playbook {playbook_filename} executed successfully.")
        except Exception as e:
            self.__log.error(e.__str__())
            return e
