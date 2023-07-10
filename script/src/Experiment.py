from os import environ
from kubernetes import client, config
import subprocess

from .utils.Defaults import DefaultValues as Value

class Experiment:
    def __init__(self):
        self.kubeconfig = config.load_kube_config(environ["KUBECONFIG"])
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
