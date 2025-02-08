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

    def create_subfolder(self, base_path, type="single_run", **kwargs):

        match type:
            case "single_run":
                subfolders = [
                    f
                    for f in os.listdir(base_path)
                    if os.path.isdir(os.path.join(base_path, f))
                ]
                subfolder_numbers = [int(f) for f in subfolders if f.isdigit()]
                next_subfolder_number = max(subfolder_numbers, default=0) + 1
                new_folder_path = os.path.join(base_path, str(next_subfolder_number))
                os.makedirs(new_folder_path)
                return new_folder_path
            case "tm":
                # Get tm_name from kwargs
                tm_name = kwargs.get("tm_name")
                # Create the res_exp folder
                new_folder_path = os.path.join(base_path, tm_name)
                os.makedirs(new_folder_path)
                return new_folder_path
            case "res_exp":
                # Get node_name from kwargs
                node_name = kwargs.get("node_name")
                # Create the res_exp folder
                new_folder_path = os.path.join(base_path, f"res_exp_{node_name}_1")
                os.makedirs(new_folder_path)
                return new_folder_path
            case _:
                self.__log.error(f"Unknown folder type: {type}")
                raise ValueError(f"Unknown folder type: {type}")

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

    def generate_grafana_quicklink(self, start_ts, end_ts) -> str:
        grafana_cluster_url = "http://grafana.monitoring.svc.cluster.local"
        start_ts = int(start_ts) * 1000
        end_ts = int(end_ts) * 1000

        # Retrieve the dashboard url
        try:
            import requests

            response = requests.get(f"{grafana_cluster_url}/api/search")
            response.raise_for_status()
            dashboards = response.json()
            dashboard_url = None
            for dashboard in dashboards:
                if dashboard["title"] == "Scalehub monitoring":
                    dashboard_url = f"{grafana_cluster_url}{dashboard['url']}"
                    break
            if dashboard_url is None:
                raise Exception("Dashboard not found")
        except Exception as e:
            self.__log.error(f"Error: {e}")
            raise e

        # Create the quicklink for localhost/grafana instead of grafana.monitoring.svc.cluster.local
        quicklink = f"http://localhost/{dashboard_url}?from={start_ts}&to={end_ts}"
        return quicklink

    def create_log_file(self, config, exp_path, start_ts, end_ts, run_number=None):
        log_file_path = os.path.join(exp_path, "exp_log.json")
        json_logs = {
            "run_number": run_number if run_number else "N/A",
            "config": config,
            "timestamps": {"start": start_ts, "end": end_ts},
            "quicklink": self.generate_grafana_quicklink(start_ts, end_ts),
        }
        try:
            import json

            with open(log_file_path, "w") as file:
                file.write(json.dumps(json_logs, indent=4))
        except Exception as e:
            self.__log.error(f"Error: {e}")
            raise e

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
        playbook_filename = (
            f"{config.get_str(Key.Scalehub.playbook.key)}/{playbook}.yaml"
        )
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
                self.__log.error(
                    f"Failed to run playbook: {playbook_filename}: {r.status}"
                )
                self.__log.error(r.stdout.read())
                return
            else:
                self.__log.info(
                    f"Playbook {playbook_filename} with tag {tags} executed successfully."
                )
        except Exception as e:
            self.__log.error(e.__str__())
            raise e
