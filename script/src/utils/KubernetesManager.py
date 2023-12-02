import os
import threading
from time import sleep

import jinja2
import yaml
from kubernetes import client as Client, config as Kubeconfig, utils
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.watch import watch

from .Logger import Logger


class KubernetesManager:
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
                f"Exception when calling AppsV1Api->read_namespaced_deployment: {e}\n"
            )
            return

        # Scale the deployment
        deployment.spec.replicas = replicas
        try:
            api_instance.patch_namespaced_deployment(
                name=deployment_name, namespace="default", body=deployment
            )
            self.__log.info(
                f"Deployment {deployment_name} scaled to {replicas} replica."
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->patch_namespaced_deployment: {e}\n"
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

    def delete_job(self, job_name):
        # Create a Kubernetes API client
        api_instance = Client.BatchV1Api()

        # Delete the job
        try:
            api_instance.delete_namespaced_job(name=job_name, namespace="default")
            self.__log.info(f"Job {job_name} deleted.")
        except Client.ApiException as e:
            self.__log.error(
                f"Exception when calling BatchV1Api->delete_namespaced_job: {e}"
            )

    # def apply_kubernetes_resource(self, filepath):
    #     try:
    #         with open(filepath, "r") as resource_file:
    #             resource_definition = resource_file.read()
    #     except FileNotFoundError as e:
    #         self.__log.error(f"Resource file not found: {e}")
    #         return
    #
    #     try:
    #         # api_instance = Client.ApiClient()
    #         # utils.create_from_dict(api_instance, yaml.safe_load(resource_definition))
    #         c
    #
    #         self.__log.info(
    #             f"Kubernetes resource from {filepath} applied to the cluster."
    #         )
    #     except ApiException as e:
    #         self.__log.error(f"Exception when applying Kubernetes resource: {e}")

    def run_job(self, filepath, params):
        try:
            with open(filepath, "r") as resource_file:
                job_template = resource_file.read()
        except FileNotFoundError as e:
            self.__log.error(f"Job template file not found: {e}")
            return

        # Render the Jinja2 template with the provided params
        rendered_job_template = jinja2.Template(job_template).render(params)

        try:
            api_instance = Client.BatchV1Api()

            job_obj = yaml.safe_load(rendered_job_template)
            job_name = job_obj["metadata"]["name"]
            namespace = job_obj["metadata"]["namespace"]

            api_instance.create_namespaced_job(namespace, job_obj)

            self.__log.info(f"Job {job_name} created in namespace {namespace}.")

            w = watch.Watch()
            for event in w.stream(
                api_instance.list_namespaced_job, namespace=namespace
            ):
                job = event["object"]
                if job.metadata.name == job_name and job.status.succeeded is not None:
                    job_log = self.get_job_logs(job_name, namespace)
                    self.__log.info(f"Job {job_name} succeeded.")
                    return job_log
        except ApiException as e:
            self.__log.error(f"Exception when running Job: {e}")

    def get_job_logs(self, job_name, namespace):
        try:
            core_v1 = core_v1_api.CoreV1Api()
            pod_list = core_v1.list_namespaced_pod(
                namespace, label_selector=f"job-name={job_name}"
            )

            if pod_list.items:
                pod_name = pod_list.items[0].metadata.name
                resp = core_v1.read_namespaced_pod_log(pod_name, namespace)
                return resp
            else:
                self.__log.error(f"No pods found for Job {job_name}.")
                return ""

        except ApiException as e:
            self.__log.error(f"Exception when getting Job logs: {e}")

    def monitor_injection_thread(self, experiment_params):
        deployment_name = "flink-taskmanager"
        reset_thread = threading.Thread(target=self.__reset_latency, args=(deployment_name,experiment_params))
        reset_thread.start()
    def __reset_latency(self, deployment_name, experiment_params):
        latency_test_file = f"/app/playbooks/project/roles/chaos/templates/flink-latency.yaml.j2"
        with open(latency_test_file) as f:
            render = jinja2.Template(f.read()).render(experiment_params)
        resource_definition = yaml.safe_load(render)

        # Create API instances
        apps_v1 = Client.AppsV1Api()
        custom_api = Client.CustomObjectsApi()

        # Watch for changes in the deployment
        stream = watch.Watch().stream(apps_v1.list_namespaced_deployment, namespace='default')
        old_replica_count = None
        for event in stream:
            deployment = event['object']
            if deployment.metadata.name == deployment_name:
                if event['type'] == 'DELETED':
                    self.__log.info("Deployment has been deleted. Exiting...")
                    break
                new_replica_count = deployment.spec.replicas
                if new_replica_count != old_replica_count:
                    self.__log.info("Detected replica change. Triggering latency experiment reset.")
                    # Delete the NetworkChaos resource
                    custom_api.delete_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        name="flink-latency"
                    )
                    sleep(3)
                    # Recreate the NetworkChaos resource

                    custom_api.create_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        body=resource_definition
                    )
                    old_replica_count = new_replica_count
    # get_token(secret_name, namespace)
    def get_token(self, secret_name, namespace):
        import base64
        core_v1 = core_v1_api.CoreV1Api()
        secret = core_v1.read_namespaced_secret(secret_name, namespace)
        token = secret.data["token"]
        # decode token from base64

        return base64.b64decode(token).decode("utf-8")