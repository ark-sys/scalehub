import os
import subprocess
from time import sleep

from kubernetes import client as Client, config as Kubeconfig

from .Logger import Logger


class Misc:
    def __init__(self, log: Logger):
        self.__log = log

    def create_credentials_file(self):

        try:
            with open("/run/secrets/mysecretuser", "r") as user_file, open(
                    "/run/secrets/mysecretpass", "r"
            ) as password_file:
                username = user_file.read().strip()
                password = password_file.read().strip()
            home_directory = os.path.expanduser("~")
            file_path = os.path.join(home_directory, ".python-grid5000.yaml")

            # Store credentials file
            with open(file_path, "w") as credentials_file:
                credentials_file.write(f"username: {username}\n")
                credentials_file.write(f"password: {password}\n")
            os.chmod(file_path, 0o600)
            self.__log.info("Credentials file created successfully.")

        except FileNotFoundError:
            self.__log.error("Error: Secrets files not found.")
        except Exception as e:
            self.__log.error(f"Error: {str(e)}")

    def run_command(self, pod_name, command):
        kubeconfig = Kubeconfig.load_kube_config(os.environ["KUBECONFIG"])

        client = Client.CoreV1Api()
        pod_list = client.list_pod_for_all_namespaces(watch=False)
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
        # Give a little bit of time for the command to complete
        sleep(5)

    def add_time(self):
        command = 'ssh -t access.grid5000.fr "ssh \\$LOGNAME@rennes"'
        add_time = " oarwalltime $(oarstat -u | tail -n 1 | cut -d ' ' -f 1) +1"
