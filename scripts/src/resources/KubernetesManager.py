import os
import threading
from time import sleep

import yaml
from kubernetes import config as kubeconfig, client as client
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.watch import watch

from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class KubernetesManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.kubeconfig = kubeconfig.load_kube_config(os.environ["KUBECONFIG"])

        self.pod_manager = PodManager(log)
        self.deployment_manager = DeploymentManager(log)
        self.service_manager = ServiceManager(log)
        self.job_manager = JobManager(log)
        self.node_manager = NodeManager(log)
        self.chaos_manager = ChaosManager(log)
        self.statefulset_manager = StatefulSetManager(log)

    # Prepare scaling when manually initiated
    def prepare_scaling(self, config, monitored_task, job_file):
        # Get current job id
        job_id = self.pod_manager.execute_command_on_pod(
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

        # # Stop current job and save returned string as savepoint path
        # resp = self.pod_manager.execute_command_on_pod(
        #     deployment_name="flink-jobmanager",
        #     command=f"flink stop {job_id}",
        # )
        # savepoint_path = None
        # for line in resp.split("\n"):
        #     if "Savepoint completed." in line:
        #         savepoint_path = line.split("Path:")[1].strip()
        #         self.__log.info(f"Savepoint path: {savepoint_path}")
        #         break

        retries = 5
        savepoint_path = None
        while retries > 0 and savepoint_path is None:
            resp = self.pod_manager.execute_command_on_pod(
                deployment_name="flink-jobmanager",
                command=f"flink stop -p -d {job_id}",
            )
            for line in resp.split("\n"):
                if "Savepoint completed." in line:
                    savepoint_path = line.split("Path:")[1].strip()
                    self.__log.info(f"Savepoint path: {savepoint_path}")
                    break
            retries -= 1
            sleep(2)
        if savepoint_path is None:
            self.__log.error("Savepoint failed.")
            return None, None, None
        sleep(5)

        return job_id, operator_names, savepoint_path

    def get_configmap(self, configmap_name, namespace="default"):
        # Create a Kubernetes API client
        api_instance = client.CoreV1Api()

        # Get the configmap
        try:
            configmap = api_instance.read_namespaced_config_map(
                name=configmap_name, namespace=namespace
            )
            return configmap.data
        except ApiException as e:
            self.__log.error(f"Exception when getting Job logs: {e}")

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


class PodManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.api_instance = client.CoreV1Api()

    def execute_command_on_pod(self, deployment_name, command):
        pod_list = self.api_instance.list_pod_for_all_namespaces(watch=False)
        target_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(deployment_name):
                target_pod = pod
                break

        if not target_pod:
            self.__log.error(
                f"[POD_MGR] No running pods found for deployment {deployment_name}"
            )
            return

        pod_name = target_pod.metadata.name

        try:
            exec_command = ["/bin/sh", "-c", command]
            self.__log.info(
                f"[POD_MGR] Running command {exec_command} on pod {pod_name}"
            )
            resp = stream(
                self.api_instance.connect_get_namespaced_pod_exec,
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
            self.__log.error(
                f"[POD_MGR] Error executing command on pod {pod_name}: {e}"
            )

    def execute_command_on_pods_by_label(
        self, label_selector, command, namespace="default"
    ):
        try:
            # Step 1: Query pods with the label "app=flink"
            pods = self.api_instance.list_namespaced_pod(
                label_selector=label_selector, namespace=namespace
            )
            for pod in pods.items:
                self.__log.info(
                    f"[POD_MGR] Running command {command} on pod {pod.metadata.name}"
                )
                # Step 2: Execute command on the pod
                self.execute_command_on_pod(pod.metadata.name, command)
        except ApiException as e:
            self.__log.error(
                f"[POD_MGR] Exception when calling CoreV1Api->list_namespaced_pod: {e}"
            )

    # Delete pods by label
    def delete_pods_by_label(self, label_selector, namespace="default"):
        try:
            # Step 1: Query pods with the label "app=flink"
            pods = self.api_instance.list_namespaced_pod(
                label_selector=label_selector, namespace=namespace
            )
            for pod in pods.items:
                # Step 2: Delete the pod
                self.api_instance.delete_namespaced_pod(
                    pod.metadata.name, pod.metadata.namespace
                )
                self.__log.info(f"[POD_MGR] Pod {pod.metadata.name} deleted")
        except ApiException as e:
            self.__log.error(
                f"[POD_MGR] Exception when calling CoreV1Api->list_namespaced_pod: {e}"
            )


class DeploymentManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.t: Tools = Tools(self.__log)
        self.api_instance = client.AppsV1Api()

    @DeprecationWarning
    def rescale_taskmanagers_heterogeneous(self, n_replicas_p, tm_type_p) -> (int, int):
        tm = "flink-taskmanager"
        tm_types = ["bm", "vm-small", "vm-medium", "pico"]

        __labels = ["node-role.kubernetes.io/worker=consumer"]

        # Create labels for the requested node type
        match tm_type_p:
            case "bm":
                __labels.append("node-role.kubernetes.io/tnode=grid5000")
            case "vm-small" | "vm-medium":
                size = tm_type_p.split("-")[1]
                __labels.append(f"node-role.kubernetes.io/vm_grid5000={size}")
                __labels.append("node-role.kubernetes.io/tnode=vm_grid5000")
            case "pico":
                __labels.append("node-role.kubernetes.io/tnode=pico")
            case _:
                self.__log.error(f"[DEP_MGR] Invalid taskmanager type: {tm_type_p}")

        # Gather current number of running replicas for each node type
        replicas = {}
        for type in tm_types:
            replicas[type] = self.get_deployment_replicas(f"{tm}-{type}", "flink")

        self.__log.info(f"[DEP_MGR] Current replicas: {replicas}")

        # Current level of parallelism is the sum of all replicas
        current_parallelism = sum(replicas.values())

        # Check if the number of replicas is already the desired one
        if replicas[tm_type_p] == n_replicas_p:
            self.__log.warning(
                f"[DEP_MGR] Number of replicas for {tm_type_p} is already {n_replicas_p}."
            )
            return 1, None
        try:

            # Create a Kubernetes API client
            api_instance = client.CoreV1Api()

            # Retrieve all nodes tha match the desired labels
            try:
                labels = ",".join(__labels)
                nodes = api_instance.list_node(label_selector=labels)
            except ApiException as e:
                self.__log.error(
                    f"[DEP_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
                )
                return 2, None

            self.__log.info(f"[DEP_MGR] Available nodes: {len(nodes.items)}")
            # Check if there are enough nodes to scale the desired node type
            if len(nodes.items) < n_replicas_p:
                self.__log.error("[DEP_MGR] Not enough available nodes to scale.")
                return 2, None

            # Relabel nodes with auta-scaling labels to ensure that the flink taskmanagers are scheduled on the correct nodes
            current_replicas = replicas[tm_type_p]
            scale_direction = "up" if n_replicas_p > current_replicas else "down"
            changes_needed = abs(n_replicas_p - current_replicas)

            self.__log.info(
                f"[DEP_MGR] Scaling {tm_type_p} from {current_replicas} to {n_replicas_p} replicas."
            )

            if scale_direction == "up":
                for node in nodes.items:
                    if changes_needed == 0:
                        break
                    autoscaling_label = node.metadata.labels.get(
                        "node-role.kubernetes.io/scaling"
                    )
                    if autoscaling_label != "SCHEDULABLE":
                        # The node is either unschedulable or does not have the autoscaling label
                        node.metadata.labels[
                            "node-role.kubernetes.io/scaling"
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
                        node.metadata.labels.get("node-role.kubernetes.io/scaling")
                        == "SCHEDULABLE"
                    ):
                        node.metadata.labels[
                            "node-role.kubernetes.io/scaling"
                        ] = "UNSCHEDULABLE"
                        api_instance.patch_node(
                            node.metadata.name,
                            {"metadata": {"labels": node.metadata.labels}},
                        )
                        changes_needed -= 1

            # Check if changes_needed is 0
            if changes_needed != 0:
                self.__log.error("[DEP_MGR] Failed to correctly relabel nodes.")
                return 2, None

            # Scale every node type to 0
            for type in tm_types:
                self.scale_deployment(f"{tm}-{type}", 0, "flink")

            # Give some time for tasks to execute
            sleep(7)

            # Scale the desired node type to the desired number of replicas and the reset back to their original number
            for type in tm_types:
                if tm_type_p == type:
                    self.scale_deployment(f"{tm}-{type}", n_replicas_p, "flink")
                else:
                    self.scale_deployment(f"{tm}-{type}", replicas[type], "flink")

            new_parallelism = sum(
                [
                    self.get_deployment_replicas(f"{tm}-{type}", "flink")
                    for type in tm_types
                ]
            )
            self.__log.info(f"[DEP_MGR] New parallelism: {new_parallelism}")

        except Exception as e:
            self.__log.error(f"[DEP_MGR] Error during rescale: {e}.")
            return 2, None
        return 0, new_parallelism

    def create_deployment(self, template_filename, params, namespace="default"):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        try:
            self.api_instance.create_namespaced_deployment(
                namespace=namespace,
                body=resource_object,
            )
            self.__log.info(
                f"[DEP_MGR] Deployment {resource_object['metadata']['name']} created."
            )

        except ApiException as e:
            self.__log.error(
                f"[DEP_MGR] Exception when calling AppsV1Api->create_namespaced_deployment: {e}\n"
            )
            return

    def delete_deployment(self, template_filename, params):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        try:
            # Check if the deployment already exists
            check_deployment = self.api_instance.read_namespaced_deployment(
                name=resource_object["metadata"]["name"],
                namespace=resource_object["metadata"]["namespace"],
            )

            if check_deployment:
                self.api_instance.delete_namespaced_deployment(
                    name=resource_object["metadata"]["name"],
                    namespace=resource_object["metadata"]["namespace"],
                )
                self.__log.info(
                    f"[DEP_MGR] Deployment {resource_object['metadata']['name']} deleted."
                )
            else:
                self.__log.info(
                    f"[DEP_MGR] Deployment {resource_object['metadata']['name']} does not exist."
                )
        except ApiException as e:
            self.__log.error(
                f"[DEP_MGR] Exception when calling AppsV1Api->delete_namespaced_deployment: {e}\n"
            )
            return

    # Scale a deployment to a specified number of replicas
    def scale_deployment(self, deployment_name, replicas=1, namespace="default"):
        # Fetch the deployment
        try:
            deployment = self.api_instance.read_namespaced_deployment(
                name=deployment_name, namespace=namespace, async_req=False
            )
        except ApiException as e:
            self.__log.error(
                f"[DEP_MGR] Exception when calling AppsV1Api->read_namespaced_deployment: {e}\n"
            )
            return

        # Scale the deployment
        # deployment.spec.replicas = replicas

        patch = {"spec": {"replicas": int(replicas)}}
        try:
            self.api_instance.patch_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=patch,
            )
            self.__log.info(
                f"[DEP_MGR] Deployment {deployment_name} scaled to {replicas} replica."
            )
        except ApiException as e:
            self.__log.error(
                f"[DEP_MGR] Exception when calling AppsV1Api->patch_namespaced_deployment: {e}\n"
            )

    def get_deployment_replicas(self, deployment_name, namespace):
        try:
            deployment = self.api_instance.read_namespaced_deployment(
                name=deployment_name, namespace=namespace, async_req=False
            )
            return int(deployment.spec.replicas)
        except ApiException as e:
            self.__log.error(
                f"[DEP_MGR] Exception when calling AppsV1Api->read_namespaced_deployment: {e}\n"
            )
            return


class ServiceManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.t: Tools = Tools(self.__log)
        self.api_instance = client.CoreV1Api()

    def create_service(self, template_filename, params, namespace="default"):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        service_name = resource_object["metadata"]["name"]

        try:
            # Check if the service already exists
            existing_service = self.api_instance.read_namespaced_service(
                name=service_name, namespace=namespace
            )
            self.__log.info(
                f"[SVC_MGR] Service {service_name} already exists. Patching the service."
            )

            # Patch the existing service
            self.api_instance.patch_namespaced_service(
                name=service_name,
                namespace=namespace,
                body=resource_object,
            )
            self.__log.info(f"[SVC_MGR] Service {service_name} patched.")
        except ApiException as e:
            if e.status == 404:
                # Service does not exist, create it
                self.api_instance.create_namespaced_service(
                    namespace=namespace, body=resource_object, async_req=False
                )
                self.__log.info(f"[SVC_MGR] Service {service_name} created.")
            else:
                self.__log.error(
                    f"[SVC_MGR] Exception when calling CoreV1Api->create_namespaced_service: {e}\n"
                )
                return

    def delete_service(self, template_filename, params):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        try:
            # Check if the service already exists
            check_service = self.api_instance.read_namespaced_service(
                name=resource_object["metadata"]["name"],
                namespace=resource_object["metadata"]["namespace"],
            )

            if check_service:
                self.api_instance.delete_namespaced_service(
                    name=resource_object["metadata"]["name"],
                    namespace=resource_object["metadata"]["namespace"],
                    async_req=False,
                )
                self.__log.info(
                    f"Service {resource_object['metadata']['name']} deleted."
                )
            else:
                self.__log.info(
                    f"Service {resource_object['metadata']['name']} does not exist."
                )

        except ApiException as e:
            self.__log.error(
                f"Exception when calling CoreV1Api->delete_namespaced_service: {e}\n"
            )
            return


class JobManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.api_instance = client.BatchV1Api()

    def delete_job(self, job_name, namespace="default"):
        # Create a Kubernetes API client

        # Delete the job
        try:
            self.api_instance.delete_namespaced_job(name=job_name, namespace=namespace)
            self.__log.info(f"Job {job_name} deleted.")
        except client.ApiException as e:
            self.__log.error(
                f"Exception when calling BatchV1Api->delete_namespaced_job: {e}"
            )

    # Retrieve job status
    def get_job_status(self, job_name, namespace="default"):

        # Get the job
        try:
            state = self.api_instance.read_namespaced_job_status(
                name=job_name, namespace=namespace
            )
            return state.status.conditions
        except client.ApiException as e:
            return e

    # Deploy a job from a yaml resource definition
    def create_job(self, resource_definition):
        try:
            resource_obj = yaml.safe_load(resource_definition)
            resource_type = resource_obj["kind"]
            resource_name = resource_obj["metadata"]["name"]
            namespace = resource_obj["metadata"]["namespace"]

            self.api_instance.create_namespaced_job(namespace, resource_obj)
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
            return ""


class NodeManager:
    node_types = ["grid5000", "vm_grid5000", "pico"]
    vm_types = ["small", "medium"]

    def __init__(self, log: Logger):
        self.__log = log
        self.api_instance = client.CoreV1Api()

    def get_nodes_by_label(self, label_selector):
        try:
            nodes = self.api_instance.list_node(label_selector=label_selector)
            return nodes.items
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return

    def get_available_worker_nodes(self):
        label_keys = ["node-role.kubernetes.io/worker=consumer"]
        for node_type in self.node_types:
            label_keys.append(f"node-role.kubernetes.io/tnode={node_type}")

        for vm_type in self.vm_types:
            label_keys.append(f"node-role.kubernetes.io/vm_grid5000={vm_type}")

        nodes = self.get_nodes_by_label(",".join(label_keys))
        nodes_count = {}
        for node in nodes:
            for label in node.metadata.labels:
                if label in label_keys:
                    if label in nodes_count:
                        nodes_count[label] += 1
                    else:
                        nodes_count[label] = 1
        return nodes_count

    def get_next_node(self, node_type, vm_type=None):
        label_keys = [
            "node-role.kubernetes.io/worker=consumer",
            f"node-role.kubernetes.io/tnode={node_type}",
        ]

        if vm_type:
            label_keys.append(f"node-role.kubernetes.io/vm_grid5000={vm_type}")

        nodes = self.get_nodes_by_label(",".join(label_keys))
        if nodes:

            # Return a node that is not yet used => it doesn't the node-role.kubernetes.io/scaling label with value SCHEDULABLE
            # And that is not full => it doesn't have the node-role.kubernetes.io/state label with value FULL
            for node in nodes:
                if (
                    node.metadata.labels.get("node-role.kubernetes.io/scaling")
                    != "SCHEDULABLE"
                    and node.metadata.labels.get("node-role.kubernetes.io/state")
                    != "FULL"
                ):
                    return node.metadata.name
        else:
            return None

    def mark_node_as_schedulable(self, node_name):
        try:
            node = self.api_instance.read_node(node_name)
            node.metadata.labels["node-role.kubernetes.io/scaling"] = "SCHEDULABLE"
            body = {"metadata": {"labels": node.metadata.labels}}
            self.api_instance.patch_node(node_name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    def mark_node_as_unschedulable(self, node_name):
        try:
            node = self.api_instance.read_node(node_name)
            node.metadata.labels["node-role.kubernetes.io/scaling"] = "UNSCHEDULABLE"
            body = {"metadata": {"labels": node.metadata.labels}}
            self.api_instance.patch_node(node_name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    def mark_node_as_full(self, node_name):
        try:
            node = self.api_instance.read_node(node_name)
            node.metadata.labels["node-role.kubernetes.io/state"] = "FULL"
            body = {"metadata": {"labels": node.metadata.labels}}
            self.api_instance.patch_node(node_name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    def get_schedulable_nodes(self):
        try:
            nodes = self.api_instance.list_node(
                label_selector="node-role.kubernetes.io/scaling=SCHEDULABLE"
            )
            return nodes.items
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    def reset_state_labels(self):
        # Remove the state label from all nodes
        try:
            nodes = self.api_instance.list_node(
                label_selector="node-role.kubernetes.io/state=FULL"
            )
            for node in nodes.items:
                node.metadata.labels["node-role.kubernetes.io/state"] = "EMPTY"
                body = {"metadata": {"labels": node.metadata.labels}}
                self.api_instance.patch_node(node.metadata.name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )

    def reset_scaling_labels(self):
        # Remove the scaling label from all nodes
        try:
            nodes = self.api_instance.list_node(
                label_selector="node-role.kubernetes.io/scaling=SCHEDULABLE"
            )
            for node in nodes.items:
                node.metadata.labels[
                    "node-role.kubernetes.io/scaling"
                ] = "UNSCHEDULABLE"
                body = {"metadata": {"labels": node.metadata.labels}}
                self.api_instance.patch_node(node.metadata.name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    # Get node by pod name
    def get_node_by_pod_name(self, pod_name, namespace="default"):
        try:
            pod = self.api_instance.read_namespaced_pod(pod_name, namespace)
            return pod.spec.node_name
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->read_namespaced_pod: {e}\n"
            )
            return None

    # Get names of nodes. If label_selector is specified, only return nodes with label
    # def get_nodes(self, label_selector):
    #     try:
    #         nodes = self.api_instance.list_node(label_selector=label_selector)
    #
    #         nodes_list = []
    #         for node in nodes.items:
    #             nodes_list.append(node.metadata.name)
    #         return nodes_list
    #     except ApiException as e:
    #         self.__log.error(f"Exception when calling CoreV1Api->list_node: {e}\n")
    #         return None

    def add_label_to_nodes(self, nodes: list, label: str):
        if not nodes or not label:
            self.__log.error("[NODE_MGR] Nodes or label is empty.")
        try:
            for node in nodes:
                # Retrieve node object
                node_object = self.api_instance.read_node(node)

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
                self.api_instance.patch_node(node_object.metadata.name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )

    # # Reset the autoscaling labels so that flink runs first on worker nodes not impacted by chaos
    # def reset_scaling_labels(self):
    #     try:
    #         # Query nodes with the label "node-role.kubernetes.io/worker=consumer"
    #         worker_label_selector = f"node-role.kubernetes.io/worker=consumer"
    #         worker_nodes = self.api_instance.list_node(
    #             label_selector=worker_label_selector
    #         )
    #         self.__log.info(f"Found {len(worker_nodes.items)} worker nodes.")
    #
    #         # Query nodes with the label chaos=true
    #         chaos_label_selector = f"chaos=true"
    #         chaos_nodes = self.api_instance.list_node(
    #             label_selector=chaos_label_selector
    #         )
    #         self.__log.info(f"Found {len(chaos_nodes.items)} chaos nodes.")
    #
    #         # Setup schedulability tag
    #         unschedulable_label = {"node-role.kubernetes.io/scaling": "UNSCHEDULABLE"}
    #         schedulable_label = {"node-role.kubernetes.io/scaling": "SCHEDULABLE"}
    #
    #         self.__log.info(f"Marking some worker nodes as schedulable")
    #         # Choas nodes are a subset of worker nodes;
    #         # Mark chaos nodes as unschedulable
    #         # Mark other worker nodes as schedulable
    #         for node in worker_nodes.items:
    #             if node.metadata.name in [
    #                 node2.metadata.name for node2 in chaos_nodes.items
    #             ]:
    #                 self.api_instance.patch_node(
    #                     node.metadata.name,
    #                     {"metadata": {"labels": unschedulable_label}},
    #                 )
    #             else:
    #                 self.api_instance.patch_node(
    #                     node.metadata.name, {"metadata": {"labels": schedulable_label}}
    #                 )
    #     except ApiException as e:
    #         self.__log.error("Error when resetting autoscaling labels.")

    def remove_label_from_nodes(self, nodes: list, label: str):
        if not nodes or not label:
            self.__log.error("[NODE_MGR] Nodes or label is empty.")
        # Create a Kubernetes API client
        try:
            for node in nodes:
                # Retrieve node object
                node_object = self.api_instance.read_node(node)

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
                self.api_instance.patch_node(node_object.metadata.name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )


class ChaosManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.t: Tools = Tools(self.__log)
        self.node_manager = NodeManager(log)
        self.api_instance = client.CustomObjectsApi()

    def deploy_networkchaos(self, experiment_params):
        # Remove label 'chaos=true' from all nodes. This is a cleanup step.
        chaos_label = "chaos=true"
        worker_nodes = self.node_manager.get_nodes_by_label(
            "node-role.kubernetes.io/worker=consumer"
        )
        self.node_manager.remove_label_from_nodes(worker_nodes, chaos_label)

        # Deploy chaos resources
        self.create_networkchaos(self.consul_chaos_template, experiment_params)

        # Wait for chaos on consul pods to be ready
        sleep(3)
        # Label nodes hosting an impacted consul pod with 'chaos=true'
        impacted_nodes = self.get_impacted_nodes()
        self.node_manager.add_label_to_nodes(impacted_nodes, chaos_label)

        # Deploy chaos resources on Flink and Storage instances running on chaos nodes
        self.create_networkchaos(self.flink_chaos_template, experiment_params)
        # self.create_networkchaos(self.storage_chaos_template, experiment_params)

        # Wait for chaos resources to be ready
        sleep(3)
        # Start thread to monitor and reset chaos injection on rescaled flink
        self.monitor_injection_thread(experiment_params)

    def monitor_injection_thread(self, experiment_params):
        deployment_name = "flink-taskmanager"
        # Start chaos injection reset thread
        self.__log.info(
            "Starting monitoring thread on scaling events. Reset chaos injection on rescale."
        )
        reset_thread = threading.Thread(
            target=self.__reset_latency, args=(deployment_name, experiment_params)
        )
        reset_thread.start()

    # Workaround to reset the latency experiment on rescale as the NetworkChaos resource does not support dynamic updates on target pods
    def __reset_latency(self, deployment_name, experiment_params):
        # Definition of the NetworkChaos resource on Flink
        resource_object = self.t.load_resource_definition(
            "/app/templates/flink-latency.yaml.j2", experiment_params
        )
        # Create API instances
        apps_v1 = client.AppsV1Api()
        # Watch for changes in the deployment
        deployment_stream = watch.Watch().stream(
            apps_v1.list_namespaced_deployment, namespace="flink"
        )
        old_replica_count = None
        for event in deployment_stream:
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
                    self.api_instance.delete_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        name="flink-latency",
                    )
                    sleep(3)

                    # Recreate the NetworkChaos resource
                    self.api_instance.create_namespaced_custom_object(
                        group="chaos-mesh.org",
                        version="v1alpha1",
                        namespace="default",
                        plural="networkchaos",
                        body=resource_object,
                    )
                    old_replica_count = new_replica_count

    # Delete all networkchaos resources
    def delete_networkchaos(self):
        try:
            self.api_instance.delete_collection_namespaced_custom_object(
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
    def create_networkchaos(self, template_filename, experiment_params):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(
            template_filename, experiment_params
        )
        try:
            self.api_instance.create_namespaced_custom_object(
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
        try:
            network_chaos_object = self.api_instance.get_namespaced_custom_object(
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
            node_names.append(
                self.node_manager.get_node_by_pod_name(instance, "consul")
            )

        return node_names


class StatefulSetManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.t: Tools = Tools(self.__log)
        self.api_instance = client.AppsV1Api()

        self.taskmanager_types = ["s", "m", "l", "xl", "xxl"]

    # Scale a statefulset to a specified number of replicas
    def scale_statefulset(self, statefulset_name, replicas=1, namespace="default"):
        # Fetch the statefulset
        try:
            statefulset = self.api_instance.read_namespaced_stateful_set(
                name=statefulset_name, namespace=namespace, async_req=False
            )
        except ApiException as e:
            self.__log.error(
                f"[STS_MGR] Exception when calling AppsV1Api->read_namespaced_stateful_set: {e}\n"
            )
            return

        # Scale the statefulset
        patch = {"spec": {"replicas": int(replicas)}}
        try:
            self.api_instance.patch_namespaced_stateful_set(
                name=statefulset_name,
                namespace=namespace,
                body=patch,
            )
            self.__log.info(
                f"[STS_MGR] StatefulSet {statefulset_name} scaled to {replicas} replica."
            )
        except ApiException as e:
            self.__log.error(
                f"[STS_MGR] Exception when calling AppsV1Api->patch_namespaced_stateful_set: {e}\n"
            )
            return

    def get_statefulset_replicas(self, statefulset_name, namespace):
        try:
            statefulset = self.api_instance.read_namespaced_stateful_set(
                name=statefulset_name, namespace=namespace, async_req=False
            )
            return int(statefulset.spec.replicas)
        except ApiException as e:
            self.__log.error(
                f"[STS_MGR] Exception when calling AppsV1Api->read_namespaced_stateful_set: {e}\n"
            )
            return

    def get_count_of_taskmanagers(self) -> dict:
        replicas = {}
        for type in self.taskmanager_types:
            replicas[type] = self.get_statefulset_replicas(
                f"flink-taskmanager-{type}", "flink"
            )
        return replicas

    def get_statefulset_by_label(self, label_selector, namespace="default"):
        try:
            statefulsets = self.api_instance.list_namespaced_stateful_set(
                namespace=namespace, label_selector=label_selector
            )
            return statefulsets.items
        except ApiException as e:
            self.__log.error(
                f"[STS_MGR] Exception when calling AppsV1Api->list_namespaced_stateful_set: {e}\n"
            )
            return None

    def reset_taskmanagers(self):
        for type in self.taskmanager_types:
            self.scale_statefulset(f"flink-taskmanager-{type}", 0, "flink")
            sleep(1)

        # Get current count of taskmanagers
        replicas = self.get_count_of_taskmanagers()

        self.__log.info(f"[STS_MGR] Current TaskManager replicas: {replicas}")
