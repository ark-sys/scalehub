import os
import re
import subprocess
from datetime import datetime
from time import sleep

import ansible_runner
import jinja2
import yaml

from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class FolderManager:
    def __init__(self, log, base_path):
        self.__log = log
        self.base_path = base_path
        self.date = self.__check_date_in_path(self.base_path)
        self.__log.info(
            f"FolderManager initialized with base path: {self.base_path}. Date: {self.date}"
        )

    def __check_date_in_path(self, path):
        # regex to match date in the path
        date_regex = r"\d{4}-\d{2}-\d{2}"
        # If we have a date in the path, return it
        if re.search(date_regex, path):
            return re.search(date_regex, path).group()
        return None

    def create_subfolder(self, base_path):
        # List all subfolders
        subfolders = [
            f
            for f in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, f))
        ]
        # Get the subfolder numbers
        subfolder_numbers = [int(f) for f in subfolders if f.isdigit()]
        # Get the next subfolder number
        next_subfolder_number = max(subfolder_numbers, default=0) + 1
        # Create the new subfolder path
        new_folder_path = os.path.join(base_path, str(next_subfolder_number))
        # Create the new subfolder
        os.makedirs(new_folder_path)
        return new_folder_path

    def create_date_folder(self, timestamp: int):
        # Convert timestamp to date string
        date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

        # Set date
        self.date = date_str

        # Date path
        date_path = os.path.join(self.base_path, date_str)

        # Create the date folder if it doesn't exist
        try:
            os.makedirs(date_path)
            return date_path
        except FileExistsError:
            return date_path

    def create_multi_run_folder(self):
        base_path = os.path.join(self.base_path, self.date)
        if not os.path.exists(base_path):
            os.makedirs(base_path)

        multi_run_folders = [
            f for f in os.listdir(base_path) if f.startswith("multi_run_")
        ]
        multi_run_numbers = [
            int(f.split("_")[-1])
            for f in multi_run_folders
            if f.split("_")[-1].isdigit()
        ]
        next_multi_run_number = max(multi_run_numbers, default=0) + 1
        multi_run_folder_path = os.path.join(
            base_path, f"multi_run_{str(next_multi_run_number)}"
        )
        os.makedirs(multi_run_folder_path)
        return multi_run_folder_path


class Tools:
    def __init__(self, log: Logger):
        self.__log = log

    def sync_data(self, experiments_path):
        # rsync command from rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc to config.get_str(Key.Scalehub.experiments)
        cmd = f"rsync -avz --ignore-existing rennes.g5k:~/scalehub-pvc/experiment-monitor-experiments-pvc/ {experiments_path}"

        self.__log.info(f"Syncing data from Grid5000 to {experiments_path}")
        # Execute the command
        subprocess.run(cmd, shell=True)

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
            self.__log.error(f"File not found: {resource_filename} - {str(e)}")
            return


class Playbooks:
    def __init__(self, log: Logger):
        self.__log = log
        self.load_generators = []

    def __load_config(self, config: Config):
        for lg_config in config.get(Key.Experiment.Generators.generators):
            load_generator_params = {
                "lg_name": lg_config["name"],
                "lg_topic": lg_config["topic"],
                "lg_type": lg_config["type"],
                "lg_numsensors": int(lg_config["num_sensors"]),
                "lg_intervalms": int(lg_config["interval_ms"]),
                "lg_replicas": int(lg_config["replicas"]),
                "lg_value": int(lg_config["value"]),
            }
            self.load_generators.append(load_generator_params)

    def role_load_generators(self, config: Config, tag=None):
        self.__load_config(config)
        # set quiet argument
        for lg in self.load_generators:
            try:
                self.run(
                    "application/load_generators",
                    config=config,
                    tag=tag,
                    extra_vars=lg,
                    quiet=True,
                )
            except Exception as e:
                self.__log.error(str(e))
                return 1
        return 0

    def reload_playbook(self, playbook, config: Config):
        self.__log.info(f"Reloading playbook: {playbook}")
        try:
            if "load_generators" in playbook:
                self.role_load_generators(config, tag="delete")
            else:
                self.run(playbook, config=config, tag="delete", quiet=True)
            sleep(5)
            if "load_generators" in playbook:
                self.role_load_generators(config, tag="create")
            else:
                self.run(playbook, config=config, tag="create", quiet=True)
        except Exception as e:
            self.__log.error(str(e))

    def run(self, playbook, config: Config, tag=None, extra_vars=None, quiet=False):
        if extra_vars is None:
            extra_vars = {}
        inventory = config.get_str(Key.Scalehub.inventory)
        playbook_filename = f"{config.get_str(Key.Scalehub.playbook)}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")
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

        # Run the playbook with additional tags and extra vars
        try:
            r = ansible_runner.run(
                private_data_dir="/tmp/ansible",
                playbook=playbook_filename,
                inventory=inventory,
                extravars=playbook_vars,
                tags=tags,
                quiet=quiet,
            )
            if r.rc != 0:
                self.__log.error(f"Failed to run playbook: {playbook_filename}")
                return r.rc
            else:
                self.__log.info(
                    f"Playbook {playbook_filename} with tag {tags} executed successfully."
                )
        except Exception as e:
            self.__log.error(e.__str__())
            return e
