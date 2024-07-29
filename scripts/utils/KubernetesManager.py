import os
import threading
from time import sleep

import jinja2
import yaml
from kubernetes import config as Kubeconfig, client as Client
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.watch import watch

from scripts.utils.Logger import Logger


class KubernetesManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.kubeconfig = Kubeconfig.load_kube_config(os.environ["KUBECONFIG"])

    # Prepare scaling when manually initiated
    def prepare_scaling(self, config, monitored_task, job_file):
        # Get current job id
        job_id = self.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="flink list -r 2>/dev/null | grep RUNNING | awk '{print $4}'",
        ).strip()

        self.__log.info(f"Job id: {job_id}")

        # Get current job operators list
        import requests

        r = requests.get(f"http://localhost/flink/jobs/{job_id}/plan")
        self.__log.info(f"Job plan response: {r.text}")
        job_plan = r.json()

        # Extract and clean operator names
        operator_names = []
        for node in job_plan["plan"]["nodes"]:
            operator_name = (
                node["description"]
                .replace("</br>", "")
                .replace("<br/>", "")
                .replace(":", "_")
                .replace(" ", "_")
            )
            operator_names.append(operator_name)

        # Stop current job and save returned string as savepoint path
        resp = self.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink stop {job_id}",
        )
        savepoint_path = None
        for line in resp.split("\n"):
            if "Savepoint completed." in line:
                savepoint_path = line.split("Path:")[1].strip()
                break
        if savepoint_path is None:
            self.__log.error("Savepoint failed.")
            return None, None, None

        sleep(15)

        return job_id, operator_names, savepoint_path

    # Scale a deployment to a specified number of replicas
    def scale_deployment(self, deployment_name, replicas=1):
        # Create a Kubernetes API client
        api_instance = Client.AppsV1Api()
        # Fetch the deployment
        try:
            deployment = api_instance.read_namespaced_deployment(
                name=deployment_name, namespace="default", async_req=False
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->read_namespaced_deployment: {e}\n"
            )
            return

        # Scale the deployment
        # deployment.spec.replicas = replicas

        patch = {"spec": {"replicas": int(replicas)}}
        try:
            api_instance.patch_namespaced_deployment(
                name=deployment_name,
                namespace="default",
                body=patch,
            )
            self.__log.info(
                f"Deployment {deployment_name} scaled to {replicas} replica."
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->patch_namespaced_deployment: {e}\n"
            )

    def create_deployment(self, template_filename, params, namespace="default"):
        # Load resource definition from file
        resource_object = self.load_resource_definition(template_filename, params)
        api_instance = Client.AppsV1Api()
        try:
            api_instance.create_namespaced_deployment(
                namespace=namespace,
                body=resource_object,
            )
            self.__log.info(
                f"Deployment {resource_object['metadata']['name']} created."
            )

        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->create_namespaced_deployment: {e}\n"
            )
            return

    def delete_deployment(self, template_filename, params):
        # Load resource definition from file
        resource_object = self.load_resource_definition(template_filename, params)
        api_instance = Client.AppsV1Api()
        try:
            api_instance.delete_namespaced_deployment(
                name=resource_object["metadata"]["name"],
                namespace=resource_object["metadata"]["namespace"],
                async_req=False,
            )
            self.__log.info(
                f"Deployment {resource_object['metadata']['name']} deleted."
            )
        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->delete_namespaced_deployment: {e}\n"
            )
            return

    def rescale_taskmanagers_heterogeneous(self, n_replicas_p, tm_type_p) -> (int, int):
        tm = "flink-taskmanager"
        tm_types = ["bm", "vm-small", "vm-medium"]

        # Taskmanagers: "bm" should run on "grid5000" nodes, "small_vm" on "grid5000-vm_small" nodes, and "medium_vm" on "grid5000-vm_medium" nodes
        # node-role.kubernetes.io/node-type
        node_roles = ["grid5000", "grid5000-vm_small", "grid5000-vm_medium"]
        target_role = node_roles[tm_types.index(tm_type_p)]

        # Gather current number of running replicas for each node type
        replicas = {}
        for type in tm_types:
            replicas[type] = self.get_deployment_replicas(f"{tm}-{type}", "default")

        self.__log.info(f"Current replicas: {replicas}")

        # Current level of parallelism is the sum of all replicas
        current_parallelism = sum(replicas.values())

        # Check if the number of replicas is already the desired one
        if replicas[tm_type_p] == n_replicas_p:
            self.__log.warning(
                f"Number of replicas for {tm_type_p} is already {n_replicas_p}."
            )
            return 1, None
        try:

            # Create a Kubernetes API client
            api_instance = Client.CoreV1Api()

            # Retrieve all nodes tha match the desired labels
            try:
                labels = ",".join(
                    [
                        f"node-role.kubernetes.io/node-type={target_role}",
                        "node-role.kubernetes.io/worker=consumer",
                    ]
                )
                nodes = api_instance.list_node(label_selector=labels)
            except ApiException as e:
                self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")
                return 2, None

            # Count the number of available nodes for each node type
            # available_nodes = {role: 0 for role in node_roles}
            # for node in nodes.items:
            #     node_type = node.metadata.labels.get(
            #         "node-role.kubernetes.io/node-type"
            #     )
            #     is_consumer = (
            #         "node-role.kubernetes.io/worker=consumer" in node.metadata.labels
            #     )
            #     if node_type in available_nodes and is_consumer:
            #         available_nodes[node_type] += 1

            self.__log.info(f"Available nodes: {len(nodes.items)}")
            # Check if there are enough nodes to scale the desired node type
            if len(nodes.items) < n_replicas_p:
                self.__log.error("Not enough available nodes to scale.")
                return 2, None

            # Relabel nodes with auta-scaling labels to ensure that the flink taskmanagers are scheduled on the correct nodes
            current_replicas = replicas[tm_type_p]
            scale_direction = "up" if n_replicas_p > current_replicas else "down"
            changes_needed = abs(n_replicas_p - current_replicas)

            self.__log.info(
                f"Scaling {tm_type_p} from {current_replicas} to {n_replicas_p} replicas."
            )

            if scale_direction == "up":
                for node in nodes.items:
                    if changes_needed == 0:
                        break
                    autoscaling_label = node.metadata.labels.get(
                        "node-role.kubernetes.io/autoscaling"
                    )
                    if autoscaling_label != "SCHEDULABLE":
                        # The node is either unschedulable or does not have the autoscaling label
                        node.metadata.labels[
                            "node-role.kubernetes.io/autoscaling"
                        ] = "SCHEDULABLE"
                        api_instance.patch_node(
                            node.metadata.name,
                            {"metadata": {"labels": node.metadata.labels}},
                        )
                        changes_needed -= 1
            else:  # scale down
                for node in nodes.items:
                    if changes_needed == 0:
                        break
                    if (
                        node.metadata.labels.get("node-role.kubernetes.io/autoscaling")
                        == "SCHEDULABLE"
                    ):
                        node.metadata.labels[
                            "node-role.kubernetes.io/autoscaling"
                        ] = "UNSCHEDULABLE"
                        api_instance.patch_node(
                            node.metadata.name,
                            {"metadata": {"labels": node.metadata.labels}},
                        )
                        changes_needed -= 1

            # Check if changes_needed is 0
            if changes_needed != 0:
                self.__log.error("Failed to correctly relabel nodes.")
                return 2, None

            # Scale every node type to 0
            for type in tm_types:
                self.scale_deployment(f"{tm}-{type}", 0)

            # Give some time for tasks to execute
            sleep(7)

            # Scale the desired node type to the desired number of replicas and the rest back to their original number
            for type in tm_types:
                if tm_type_p == type:
                    self.scale_deployment(f"{tm}-{type}", n_replicas_p)
                else:
                    self.scale_deployment(f"{tm}-{type}", replicas[type])

            new_parallelism = sum(
                [
                    self.get_deployment_replicas(f"{tm}-{type}", "default")
                    for type in tm_types
                ]
            )
            self.__log.info(f"New parallelism: {new_parallelism}")

        except Exception as e:
            self.__log.error(f"Error during rescale: {e}")
            return 2, None
        return 0, new_parallelism

    def get_deployment_replicas(self, deployment_name, namespace):
        api_instance = Client.AppsV1Api()
        try:
            deployment = api_instance.read_namespaced_deployment(
                name=deployment_name, namespace=namespace, async_req=False
            )
            return int(deployment.spec.replicas)
        except ApiException as e:
            self.__log.error(
                f"Exception when calling AppsV1Api->read_namespaced_deployment: {e}\n"
            )
            return

    def create_service(self, template_filename, params, namespace="default"):
        # Load resource definition from file
        resource_object = self.load_resource_definition(template_filename, params)
        api_instance = Client.CoreV1Api()
        try:

            # Create service
            api_instance.create_namespaced_service(
                namespace=namespace, body=resource_object, async_req=False
            )
            self.__log.info(f"Service {resource_object['metadata']['name']} created.")
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->create_namespaced_service: {e}\n"
            )
            return

    def delete_service(self, template_filename, params):
        # Load resource definition from file
        resource_object = self.load_resource_definition(template_filename, params)
        api_instance = Client.CoreV1Api()
        try:
            api_instance.delete_namespaced_service(
                name=resource_object["metadata"]["name"],
                namespace=resource_object["metadata"]["namespace"],
                async_req=False,
            )
            self.__log.info(f"Service {resource_object['metadata']['name']} deleted.")
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->delete_namespaced_service: {e}\n"
            )
            return

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
            state = api_instance.read_namespaced_job_status(
                name=job_name, namespace="default"
            )
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
        resource_object = self.load_resource_definition(
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
                        body=resource_object,
                    )
                    old_replica_count = new_replica_count

        # Get node by pod name

    def get_node_by_pod_name(self, pod_name, namespace="default"):
        v1 = Client.CoreV1Api()
        try:
            pod = v1.read_namespaced_pod(pod_name, namespace)
            return pod.spec.node_name
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->read_namespaced_pod: {e}\n"
            )
            return None

        # Get names of nodes. If label_selector is specified, only return nodes with label

    def get_nodes(self, label_selector):
        v1 = Client.CoreV1Api()
        try:
            nodes = v1.list_node(label_selector=label_selector)

            nodes_list = []
            for node in nodes.items:
                nodes_list.append(node.metadata.name)
            return nodes_list
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
        if not nodes or not label:
            self.__log.error("Nodes or label is empty")
        # Create a Kubernetes API client
        api_instance = Client.CoreV1Api()

        try:
            for node in nodes:
                # Retrieve node object
                node_object = api_instance.read_node(node)

                node_labels = node_object.metadata.labels.copy()
                # Get label from str format to dict format and add label to node
                # Check if label is key=value format or just key format
                if "=" in label:
                    key, value = label.split("=")
                    node_labels[key] = value
                else:
                    node_labels[label] = ""
                body = {"metadata": {"labels": node_labels}}
                # Patch node with new label
                api_instance.patch_node(node_object.metadata.name, body=body)
        except ApiException as e:
            self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")

    def remove_label_from_nodes(self, nodes: list, label: str):
        if not nodes or not label:
            self.__log.error("Nodes or label is empty")
        # Create a Kubernetes API client
        api_instance = Client.CoreV1Api()
        try:
            for node in nodes:
                # Retrieve node object
                node_object = api_instance.read_node(node)

                node_labels = node_object.metadata.labels.copy()
                # Check if label is key=value format or just key format, and remove label from node by setting value to None
                if "=" in label:
                    key, value = label.split("=")
                    # Check if label exists
                    if key in node_object.metadata.labels:
                        # Update value of key to None
                        node_labels[key] = None
                else:
                    # Check if label exists
                    if label in node_object.metadata.labels:
                        # Update value of key to None
                        node_labels[label] = None
                body = {"metadata": {"labels": node_labels}}
                # Patch node with new label
                api_instance.patch_node(node_object.metadata.name, body=body)
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
                resource_template = f.read()
                resource_definition = jinja2.Template(resource_template).render(
                    experiment_params
                )
            resource_object = yaml.safe_load(resource_definition)
            return resource_object
        except FileNotFoundError as e:
            self.__log.error(f"File not found: {resource_filename}")
            return

        # Deploy a networkchaos resource

    def create_networkchaos(self, template_filename, experiment_params):
        # Load resource definition from file
        resource_object = self.load_resource_definition(
            template_filename, experiment_params
        )
        custom_api = Client.CustomObjectsApi()
        try:
            custom_api.create_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace="default",
                plural="networkchaos",
                body=resource_object,
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
            network_chaos_object = custom_api.get_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace="default",
                plural="networkchaos",
                name="consul-latency",
            )

            result = [
                instance.split("/")[1]
                for instance in network_chaos_object["status"]["instances"]
            ]
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
            node_names.append(self.get_node_by_pod_name(instance, "consul"))

        return node_names

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

    # get_token(secret_name, namespace)
    def get_token(self, secret_name, namespace):
        import base64

        try:
            core_v1 = core_v1_api.CoreV1Api()
            secret = core_v1.read_namespaced_secret(secret_name, namespace)
            token = secret.data["token"]
        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->read_namespaced_secret: {e}"
            )
            return

        # decode token from base64
        return base64.b64decode(token).decode("utf-8")
