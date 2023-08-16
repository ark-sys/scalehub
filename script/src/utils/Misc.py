import os
import re
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

    def parse_log(self, log_path: str):
        with open(log_path, "r") as log_file:
            logs = log_file.read()

        job_name_match = re.search(r"Job name : (.+)", logs)
        lg_matches = re.finditer(
            r"LG : (.+?)\s+topic : (.+?)\s+num_sensors : (\d+)\s+interval_ms : (\d+)",
            logs,
            re.DOTALL,
        )
        start_match = re.search(r"Experiment start at : (\d+)", logs)
        end_match = re.search(r"Experiment end at : (\d+)", logs)
        if job_name_match:
            job_name = job_name_match.group(1)
        else:
            self.__log.error("Job name not found in log.")
            exit(1)
        if start_match and end_match:
            start_timestamp = int(start_match.group(1))
            end_timestamp = int(end_match.group(1))
        else:
            self.__log.error("Log file is incomplete: missing timestamp.")
            exit(1)

        num_sensors_sum = 0
        interval_ms_sum = 0
        lg_count = 0

        for lg_match in lg_matches:
            num_sensors = int(lg_match.group(3))
            interval_ms = int(lg_match.group(4))
            num_sensors_sum += num_sensors
            interval_ms_sum += interval_ms
            lg_count += 1

        if lg_count == 0:
            self.__log.error("No LG information found in log.")
            exit(1)

        avg_interval_ms = interval_ms_sum / lg_count

        return (
            job_name,
            num_sensors_sum,
            avg_interval_ms,
            start_timestamp,
            end_timestamp,
        )
