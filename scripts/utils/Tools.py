import os
import subprocess
from datetime import datetime

import jinja2
import yaml

from scripts.utils.Logger import Logger


class FolderManager:
    def __init__(self, log, base_path):
        self.__log = log
        self.base_path = base_path
        self.date = datetime.fromtimestamp(int(datetime.now().timestamp())).strftime(
            "%Y-%m-%d"
        )
        self.date_path = os.path.join(self.base_path, self.date)
        self.__log.info(
            f"FolderManager initialized with base path: {self.base_path}. Date: {self.date}"
        )

    def create_subfolder(self, base_path, subfolder_type="single_run", **kwargs):
        try:
            match subfolder_type:
                case "single_run":
                    subfolders = [
                        f
                        for f in os.listdir(base_path)
                        if os.path.isdir(os.path.join(base_path, f))
                    ]
                    subfolder_numbers = [int(f) for f in subfolders if f.isdigit()]
                    next_subfolder_number = max(subfolder_numbers, default=0) + 1
                    new_folder_path = os.path.join(
                        base_path, str(next_subfolder_number)
                    )
                    os.makedirs(new_folder_path, exist_ok=True)
                    return new_folder_path
                case "tm":
                    # Get tm_name from kwargs
                    tm_name = kwargs.get("tm_name")
                    # Create the res_exp folder
                    new_folder_path = os.path.join(base_path, tm_name)
                    os.makedirs(new_folder_path, exist_ok=True)
                    return new_folder_path
                case "res_exp":
                    # Get node_name from kwargs
                    node_name = kwargs.get("node_name")
                    # Create the res_exp folder
                    # Find the next available folder number
                    existing_folders = [
                        f
                        for f in os.listdir(base_path)
                        if f.startswith(f"res_exp_{node_name}_")
                    ]
                    folder_numbers = [
                        int(f.split("_")[-1])
                        for f in existing_folders
                        if f.split("_")[-1].isdigit()
                    ]
                    next_folder_number = max(folder_numbers, default=1) + 1
                    new_folder_path = os.path.join(
                        base_path, f"res_exp_{node_name}_{next_folder_number}"
                    )
                    os.makedirs(new_folder_path, exist_ok=True)
                    return new_folder_path
                case _:
                    self.__log.error(f"Unknown folder type: {subfolder_type}")
                    raise ValueError(f"Unknown folder type: {subfolder_type}")
        except Exception as e:
            self.__log.error(f"Error: {e}")

    def create_date_folder(self):
        # Create the date folder if it doesn't exist
        try:
            os.makedirs(self.date_path)
            return self.date_path
        except FileExistsError:
            return self.date_path
        except Exception as e:
            raise e

    def create_multi_run_folder(self):
        if not os.path.exists(self.date_path):
            os.makedirs(self.date_path)

        multi_run_folders = [
            f for f in os.listdir(self.date_path) if f.startswith("multi_run_")
        ]
        multi_run_numbers = [
            int(f.split("_")[-1])
            for f in multi_run_folders
            if f.split("_")[-1].isdigit()
        ]
        next_multi_run_number = max(multi_run_numbers, default=0) + 1
        multi_run_folder_path = os.path.join(
            self.date_path, f"multi_run_{str(next_multi_run_number)}"
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

    def generate_grafana_quicklink(self, start_ts, end_ts) -> (str, str):
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
        quicklink_local = (
            f"http://localhost/{dashboard_url}?from={start_ts}&to={end_ts}"
        )
        quicklink_remote = (
            f"http://grafana.scalehub.dev/{dashboard_url}?from={start_ts}&to={end_ts}"
        )
        return quicklink_local, quicklink_remote

    def create_log_file(self, config, exp_path, start_ts, end_ts, run_number=None):
        log_file_path = os.path.join(exp_path, "exp_log.json")
        q1, q2 = self.generate_grafana_quicklink(start_ts, end_ts)
        json_logs = {
            "run_number": run_number if run_number else "N/A",
            "config": config,
            "timestamps": {"start": start_ts, "end": end_ts},
            "quicklink_local": q1,
            "quicklink_remote": q2,
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
            return None
