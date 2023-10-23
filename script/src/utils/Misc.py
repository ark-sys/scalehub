import os
import re
import yaml
from kubernetes import client as Client, config as Kubeconfig, utils
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from .Logger import Logger


class Misc:
    def __init__(self, log: Logger):
        self.__log = log
        self.kubeconfig = Kubeconfig.load_kube_config(os.environ["KUBECONFIG"])

    def scale_deployment(self, deployment_name, replicas=1):
        # Create a Kubernetes API client
        api_instance = Client.AppsV1Api()

        # Fetch the deployment
        try:
            deployment = api_instance.read_namespaced_deployment(
                name=deployment_name, namespace="default"
            )
        except ApiException as e:
            self.__log.error(
                "Exception when calling AppsV1Api->read_namespaced_deployment: %s\n" % e
            )
            return

        # Scale down the deployment
        deployment.spec.replicas = replicas
        try:
            api_instance.patch_namespaced_deployment(
                name=deployment_name, namespace="default", body=deployment
            )
            self.__log.info(f"Deployment {deployment_name} scaled down to 1 replica.")
        except ApiException as e:
            self.__log.error(
                "Exception when calling AppsV1Api->patch_namespaced_deployment: %s\n"
                % e
            )
            return

    def delete_job(self, job_name):
        # Create a Kubernetes API client
        api_instance = Client.BatchV1Api()

        # Delete the job
        try:
            api_instance.delete_namespaced_job(name=job_name, namespace="default")
            self.__log.info(f"Job {job_name} deleted.")
        except Client.ApiException as e:
            self.__log.error(
                "Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e
            )

    def execute_command_on_pod(self, deployment_name, command):

        try:
            c = Configuration().get_default_copy()
        except AttributeError:
            c = Configuration()
            c.assert_hostname = False
        Configuration.set_default(c)

        core_v1 = core_v1_api.CoreV1Api()

        pod_list = core_v1.list_pod_for_all_namespaces(watch=False)
        target_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(deployment_name):
                target_pod = pod
                break

        if not target_pod:
            self.__log.error(f"No running pods found for deployment {deployment_name}")
            return

        pod_name = target_pod.metadata.name

        try:
            exec_command = ["/bin/sh", "-c", command]
            self.__log.info(f"Running command {exec_command} on pod {pod_name}")
            resp = stream(
                core_v1.connect_get_namespaced_pod_exec,
                name=pod_name,
                namespace=target_pod.metadata.namespace,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )

            return resp  # Return the captured output
        except ApiException as e:
            self.__log.error(f"Error executing command on pod {pod_name}: {e}")

    def apply_kubernetes_resource(self, file_path):
        """
        Apply Kubernetes resource from the specified file path.

        Args:
            file_path (str): Path to the file containing Kubernetes resource.

        Returns:
            bool: True if the resource was applied successfully, False otherwise.
        """
        try:
            api_instance = Client.ApiClient()

            # Apply each resource
            utils.create_from_yaml(api_instance, file_path)
            self.__log.info("Kubernetes resource applied successfully.")
        except ApiException as e:
            self.__log.error(f"Error applying Kubernetes resource: {str(e)}")
            return

        except Exception as e:
            self.__log.error(f"An unexpected error occurred: {str(e)}")
            return

    def add_time(self):
        command = 'ssh -t access.grid5000.fr "ssh \\$LOGNAME@rennes"'
        add_time = " oarwalltime $(oarstat -u | tail -n 1 | cut -d ' ' -f 1) +1"

    def parse_log(self, log_path: str):
        with open(log_path, "r") as log_file:
            logs = log_file.read()

        job_name_match = re.search(r"experiment.job_file = (.+)", logs)
        lg_matches = re.finditer(
            r"name = (.+?)\s+topic = (.+?)\s+num_sensors = (\d+)\s+interval_ms = (\d+)",
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
