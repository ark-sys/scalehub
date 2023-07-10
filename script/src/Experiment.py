from kubernetes import client, config as kubeconfig
import subprocess
from datetime import date
import os

from .utils.Config import Key as Key, Config

from .utils.Defaults import DefaultValues as Value


class Experiment:
    def __init__(self):
        self.kubeconfig = kubeconfig.load_kube_config(os.environ["KUBECONFIG"])
        self.client = client.CoreV1Api()

    def run_command(self, pod_name, command):
        pod_list = self.client.list_pod_for_all_namespaces(watch=False)
        target_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(pod_name):
                target_pod = pod
                break

        if not target_pod:
            raise ValueError(f"No pod found with name: {pod_name}")

        # Execute the shell command on the pod
        command = f"kubectl exec -it {target_pod.metadata.name} -- {command}"
        subprocess.call(command, shell=True)

    # Create a folder for the experiments data
    def create_exp_folder(self, config: Config):
        # Get the current date in the format DD-MM-YYYY
        current_date = date.today().strftime('%d-%m-%Y')

        # Create the base folder path
        base_folder_path = os.path.join(config.get_str(Key.EXPERIMENTS_DATA_PATH), current_date)

        # Create the base folder if it doesn't exist
        if not os.path.exists(base_folder_path):
            os.makedirs(base_folder_path)

        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1

        # Create the subfolder
        os.makedirs(subfolder_path)

        # Return the path to the new subfolder
        return subfolder_path

    def export_data(self):
        pass
