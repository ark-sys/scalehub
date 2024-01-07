import os
import threading
from time import sleep

import yaml
from kubernetes import client as Client, config as Kubeconfig
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

    # Scale a deployment to a specified number of replicas
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

    # Execute a command on first pod of a deployment
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

    # Execute a command on all pods of a deployment by label
    def execute_command_on_pods_by_label(self, label_selector, command):
        core_v1 = core_v1_api.CoreV1Api()
        try:
            # Step 1: Query pods with the label "app=flink"
            pods = core_v1.list_namespaced_pod(
                label_selector=label_selector, namespace="default"
            )
            for pod in pods.items:
                self.__log.info(f"Running command {command} on pod {pod.metadata.name}")
                # Step 2: Execute command on the pod
                self.execute_command_on_pod(pod.metadata.name, command)
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->list_namespaced_pod: {e}"
            )

    # Delete job by name
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

    # Retrieve job status
    def get_job_status(self, job_name):
        # Create a Kubernetes API client
        api_instance = Client.BatchV1Api()

        # Get the job
        try:
            state = api_instance.read_namespaced_job_status(name=job_name, namespace="default")
            return state.status.conditions
        except Client.ApiException as e:
            return e

    # Retrieve content of a configmap
    def get_configmap(self, configmap_name, namespace="default"):
        # Create a Kubernetes API client
        api_instance = Client.CoreV1Api()

        # Get the configmap
        try:
            configmap = api_instance.read_namespaced_config_map(
                name=configmap_name, namespace=namespace
            )
            return configmap.data
        except Client.ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->read_namespaced_config_map: {e}"
            )

    # Deploy a job from a yaml resource definition
    def create_job(self, resource_definition):
        try:
            api_instance = Client.BatchV1Api()

            resource_obj = yaml.safe_load(resource_definition)
            resource_type = resource_obj["kind"]
            resource_name = resource_obj["metadata"]["name"]
            namespace = resource_obj["metadata"]["namespace"]

            api_instance.create_namespaced_job(namespace, resource_obj)
            self.__log.info(
                f"{resource_type} {resource_name} created in namespace {namespace}."
            )
        except ApiException as e:
            self.__log.error(f"Exception when operating on resource: {e}")

    def get_job_logs(self, job_name, namespace):
        try:
            core_v1 = core_v1_api.CoreV1Api()
            pod_list = core_v1.list_namespaced_pod(
                namespace, label_selector=f"job-name={job_name}"
            )
            logs = []
            if pod_list.items:
                for pod in pod_list.items:
                    logs.append(
                        core_v1.read_namespaced_pod_log(pod.metadata.name, namespace)
                    )
                return "\n".join(logs)
            else:
                self.__log.error(f"No pods found for Job {job_name}.")
                return ""

        except ApiException as e:
            self.__log.error(f"Exception when getting Job logs: {e}")

    def monitor_injection_thread(self, experiment_params):
        deployment_name = "flink-taskmanager"
        reset_thread = threading.Thread(
            target=self.__reset_latency, args=(deployment_name, experiment_params)
        )
        reset_thread.start()

    # Workaround to reset the latency experiment on rescale as the NetworkChaos resource does not support dynamic updates on target pods
    def __reset_latency(self, deployment_name, experiment_params):
        # Definition of the NetworkChaos resource on Flink
        resource_definition = self.load_resource_definition(
            "/app/templates/flink-latency.yaml.j2", experiment_params
        )
        # Create API instances
        apps_v1 = Client.AppsV1Api()
        custom_api = Client.CustomObjectsApi()

        # Watch for changes in the deployment
        stream = watch.Watch().stream(
            apps_v1.list_namespaced_deployment, namespace="default"
        )
        old_replica_count = None
        for event in stream:
            deployment = event["object"]
            if deployment.metadata.name == deployment_name:
                if event["type"] == "DELETED":
                    self.__log.info("Deployment has been deleted. Exiting...")
                    break
                new_replica_count = deployment.spec.replicas
                if new_replica_count != old_replica_count:
                    self.__log.info(
                        "Detected replica change. Triggering latency experiment reset."
                    )
                    # Delete the NetworkChaos resource
                    custom_api.delete_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        name="flink-latency",
                    )
                    sleep(3)
                    # Recreate the NetworkChaos resource

                    custom_api.create_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        body=resource_definition,
                    )
                    old_replica_count = new_replica_count
    # Get node by pod name
    def get_node_by_pod_name(self, pod_name, namespace="default"):
        v1 = Client.CoreV1Api()
        try:
            pod = v1.read_namespaced_pod(pod_name, namespace)
            return pod.spec.node_name
        except ApiException as e:
            self.__log.error(f"Exception when calling CoreV1Api->read_namespaced_pod: {e}\n")
            return None
    def get_nodes_by_label(self, label_selector, namespace="default"):
        v1 = Client.CoreV1Api()
        try:
            nodes = v1.list_node(label_selector=label_selector)
            return nodes.items
        except ApiException as e:
            self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")
            return None
    # Delete pods by label
    def delete_pods_by_label(self, label_selector, namespace="default"):
        v1 = Client.CoreV1Api()
        try:
            # Step 1: Query pods with the label "app=flink"
            pods = v1.list_namespaced_pod(
                label_selector=label_selector, namespace=namespace
            )
            for pod in pods.items:
                # Step 2: Delete the pod
                v1.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
                self.__log.info(f"Pod {pod.metadata.name} deleted")
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->list_namespaced_pod: {e}"
            )

    # Reset the autoscaling labels so that flink runs first on worker nodes not impacted by chaos
    def reset_autoscaling_labels(self):
        v1 = Client.CoreV1Api()
        try:
            # Query nodes with the label "node-role.kubernetes.io/worker=consumer"
            worker_label_selector = f"node-role.kubernetes.io/worker=consumer"
            worker_nodes = v1.list_node(label_selector=worker_label_selector)
            self.__log.info(f"Found {len(worker_nodes.items)} worker nodes.")

            # Query nodes with the label chaos=true
            chaos_label_selector = f"chaos=true"
            chaos_nodes = v1.list_node(label_selector=chaos_label_selector)
            self.__log.info(f"Found {len(chaos_nodes.items)} chaos nodes.")

            # Setup schedulability tag
            unschedulable_label = {
                "node-role.kubernetes.io/autoscaling": "UNSCHEDULABLE"
            }
            schedulable_label = {"node-role.kubernetes.io/autoscaling": "SCHEDULABLE"}

            self.__log.info(f"Marking some worker nodes as schedulable")
            # Choas nodes are a subset of worker nodes;
            # Mark chaos nodes as unschedulable
            # Mark other worker nodes as schedulable
            for node in worker_nodes.items:
                if node.metadata.name in [
                    node2.metadata.name for node2 in chaos_nodes.items
                ]:
                    v1.patch_node(
                        node.metadata.name,
                        {"metadata": {"labels": unschedulable_label}},
                    )
                else:
                    v1.patch_node(
                        node.metadata.name, {"metadata": {"labels": schedulable_label}}
                    )
        except ApiException as e:
            self.__log.error("Error when resetting autoscaling labels.")

    def add_label_to_nodes(self, nodes: list, label: str):
        # Create a Kubernetes API client
        api_instance = Client.CoreV1Api()

        try:
            for node in nodes:
                self.__log.info(f"Adding label {label} to node {node}")
                node.metadata.labels.update(label)
                api_instance.patch_node(node.metadata.name, {"metadata": {"labels": node.metadata.labels}})
        except ApiException as e:
            self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")


    def remove_label_from_nodes(self, nodes: list, label: str):
        # Create a Kubernetes API client
        api_instance = Client.CoreV1Api()

        if not nodes:
            nodes = api_instance.list_node(label_selector=label)
        try:
            for node in nodes:
                self.__log.info(f"Removing label {label} from node {node}")
                node.metadata.labels.pop(label)
                api_instance.patch_node(node.metadata.name, {"metadata": {"labels": node.metadata.labels}})
        except ApiException as e:
            self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")
    # Delete all networkchaos resources
    def delete_networkchaos(self):
        custom_api = Client.CustomObjectsApi()
        try:
            custom_api.delete_collection_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace="default",
                plural="networkchaos",
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CustomObjectsApi->delete_collection_namespaced_custom_object: {e}\n"
            )
            return
        self.__log.info("NetworkChaos resources deleted.")

    # Load resource definition template from file and fill in the parameters
    def load_resource_definition(self, resource_filename, experiment_params):

        try:
            with open(resource_filename, "r") as f:
                resource_definition = yaml.load(f, Loader=yaml.FullLoader)
                resource_definition["experiment-params"] = experiment_params
                return resource_definition
        except FileNotFoundError as e:
            self.__log.error(f"File not found: {resource_filename}")
            return
    # Deploy a networkchaos resource
    def create_networkchaos(self, template_filename, experiment_params):
        # Load resource definition from file
        resource_definition = self.load_resource_definition(template_filename, experiment_params)
        custom_api = Client.CustomObjectsApi()
        try:
            custom_api.create_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace="default",
                plural="networkchaos",
                body=resource_definition,
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CustomObjectsApi->create_namespaced_custom_object: {e}\n"
            )
            return
        self.__log.info("NetworkChaos resource created.")



    # Get pods impacted by networkchaos
    def get_networkchaos_instances(self):
        custom_api = Client.CustomObjectsApi()
        try:
            network_chaos_objects = custom_api.list_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace="default",
                plural="networkchaos",
            )

            result = [instance.split("/")[1] for instance in network_chaos_objects["items"][0]['status']['instances']]
            return result
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CustomObjectsApi->list_namespaced_custom_object: {e}\n"
            )
            return
    # Get node names of impacted consul pods
    def get_impacted_nodes(self):

        instances = self.get_networkchaos_instances()
        node_names = []

        for instance in instances:
            node_names.append(self.get_node_by_pod_name(instance,"consul"))

        return node_names