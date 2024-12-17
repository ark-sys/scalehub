import os
from time import sleep

import yaml
from kubernetes import config as kubeconfig, client as client
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools


class KubernetesManager:
    def __init__(self, log: Logger):
        self.__log = log
        try:
            self.kubeconfig = kubeconfig.load_kube_config(os.environ["KUBECONFIG"])
        except Exception as e:
            self.__log.warning(f"Error loading kubeconfig from ENV: {e}")
            self.__log.warning("Trying incluster config instead.")
            try:
                self.kubeconfig = kubeconfig.load_incluster_config()
            except Exception as e:
                self.__log.error(f"Error loading incluster kubeconfig: {e}")
                self.__log.error("Could not find a valid kubeconfig. Exiting.")

        self.pod_manager = PodManager(log)
        self.deployment_manager = DeploymentManager(log)
        self.service_manager = ServiceManager(log)
        self.job_manager = JobManager(log)
        self.node_manager = NodeManager(log)
        self.statefulset_manager = StatefulSetManager(log)
        # self.chaos_manager = ChaosManager(log)

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

    # Check if pod is running and ready
    def is_pod_ready(self, pod_name, namespace="default"):
        try:
            pod = self.api_instance.read_namespaced_pod_status(pod_name, namespace)
            if pod.status.phase == "Running":
                for condition in pod.status.conditions:
                    if condition.type == "Ready" and condition.status == "True":
                        return True
            return False
        except ApiException as e:
            self.__log.error(
                f"[POD_MGR] Exception when calling CoreV1Api->read_namespaced_pod_status: {e}"
            )
            return False

    def get_logs_since(self, label_selector, time, namespace="default"):
        # Time in seconds must be greater than 0
        if time <= 0:
            self.__log.error("[POD_MGR] Time must be greater than 0.")
            return ""
        else:
            try:
                pods = self.api_instance.list_namespaced_pod(
                    label_selector=label_selector, namespace=namespace
                )
                logs = []
                for pod in pods.items:
                    logs.append(
                        self.api_instance.read_namespaced_pod_log(
                            pod.metadata.name,
                            namespace,
                            since_seconds=time,
                            pretty=True,
                        )
                    )
                return "\n".join(logs)
            except ApiException as e:
                self.__log.error(f"[POD_MGR] Exception when getting logs: {e}")
                return ""


class DeploymentManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.t: Tools = Tools(self.__log)
        self.api_instance = client.AppsV1Api()

    def create_deployment_from_template(
        self, template_filename, params, namespace="default"
    ):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        try:
            # Patch the existing deployment
            self.api_instance.patch_namespaced_deployment(
                name=resource_object["metadata"]["name"],
                namespace=resource_object["metadata"]["namespace"],
                body=resource_object,
            )

        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, create it
                self.api_instance.create_namespaced_deployment(
                    namespace=resource_object["metadata"]["namespace"],
                    body=resource_object,
                    async_req=False,
                )
                self.__log.info(
                    f"[DEP_MGR] Deployment {resource_object['metadata']['name']} created."
                )
            return

    def delete_deployment_from_template(self, template_filename, params):
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
                    async_req=False,
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

    def create_service_from_template(
        self, template_filename, params, namespace="default"
    ):
        # Load resource definition from file
        resource_object = self.t.load_resource_definition(template_filename, params)
        service_name = resource_object["metadata"]["name"]

        try:
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

    def delete_service_from_template(self, template_filename, params):
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
                    f"[SVC_MGR] Service {resource_object['metadata']['name']} deleted."
                )
            else:
                self.__log.info(
                    f"[SVC_MGR] Service {resource_object['metadata']['name']} does not exist."
                )

        except ApiException as e:
            self.__log.error(
                f"[SVC_MGR] Exception when calling CoreV1Api->delete_namespaced_service: {e}\n"
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

    def node_list(self, label_selector):
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

        nodes = self.node_list(",".join(label_keys))
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

        nodes = self.node_list(",".join(label_keys))
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

    def mark_node(self, node_name, label, value):
        try:
            node = self.api_instance.read_node(node_name)
            node.metadata.labels[label] = value
            body = {"metadata": {"labels": node.metadata.labels}}
            self.api_instance.patch_node(node_name, body=body)
        except ApiException as e:
            self.__log.error(
                f"[NODE_MGR] Exception when calling CoreV1Api->list_node: {e}\n"
            )
            return None

    def mark_node_as_schedulable(self, node_name):
        label = "node-role.kubernetes.io/scaling"
        value = "SCHEDULABLE"
        self.mark_node(node_name, label, value)

    def mark_node_as_unschedulable(self, node_name):
        label = "node-role.kubernetes.io/scaling"
        value = "UNSCHEDULABLE"
        self.mark_node(node_name, label, value)

    def mark_node_as_empty(self, node_name):
        label = "node-role.kubernetes.io/state"
        value = "EMPTY"
        self.mark_node(node_name, label, value)

    def mark_node_as_full(self, node_name):
        label = "node-role.kubernetes.io/state"
        value = "FULL"
        self.mark_node(node_name, label, value)

    def get_schedulable_nodes(self):
        label = "node-role.kubernetes.io/scaling=SCHEDULABLE"
        return self.node_list(label)

    def reset_state_labels(self):
        self.__log.info("[NODE_MGR] Resetting state labels.")
        # Get all nodes and mark them as empty
        nodes = self.node_list("node-role.kubernetes.io/state=FULL")
        for node in nodes:
            self.mark_node_as_empty(node.metadata.name)

    def reset_scaling_labels(self):
        self.__log.info("[NODE_MGR] Resetting scaling labels to unschedulable.")
        # Get all nodes and mark them as schedulable
        nodes = self.get_schedulable_nodes()
        for node in nodes:
            self.mark_node_as_unschedulable(node.metadata.name)


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

            # Wait until the statefulset is scaled
            while (
                self.get_statefulset_replicas(statefulset_name, namespace) != replicas
            ):
                sleep(1)

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

    def get_count_of_taskmanagers(self) -> dict:
        replicas = {}
        for type in self.taskmanager_types:
            replicas[type] = self.get_statefulset_replicas(
                f"flink-taskmanager-{type}", "flink"
            )
        return replicas

    def reset_taskmanagers(self):
        for type in self.taskmanager_types:
            self.scale_statefulset(f"flink-taskmanager-{type}", 0, "flink")
            sleep(1)
        # Wait until all taskmanagers are terminated
        while sum(self.get_count_of_taskmanagers()) > 0:
            sleep(5)


# class ChaosManager:
#    def __init__(self, log: Logger):
#        self.__log = log
#        self.t: Tools = Tools(self.__log)
#        self.node_manager = NodeManager(log)
#        self.api_instance = client.CustomObjectsApi()
#
#    def deploy_networkchaos(self, experiment_params):
#        # Remove label 'chaos=true' from all nodes. This is a cleanup step.
#        chaos_label = "chaos=true"
#        worker_nodes = self.node_manager.get_nodes_by_label(
#            "node-role.kubernetes.io/worker=consumer"
#        )
#        self.node_manager.remove_label_from_nodes(worker_nodes, chaos_label)
#
#        # Deploy chaos resources
#        self.create_networkchaos(self.consul_chaos_template, experiment_params)
#
#        # Wait for chaos on consul pods to be ready
#        sleep(3)
#        # Label nodes hosting an impacted consul pod with 'chaos=true'
#        impacted_nodes = self.get_impacted_nodes()
#        self.node_manager.add_label_to_nodes(impacted_nodes, chaos_label)
#
#        # Deploy chaos resources on Flink and Storage instances running on chaos nodes
#        self.create_networkchaos(self.flink_chaos_template, experiment_params)
#        # self.create_networkchaos(self.storage_chaos_template, experiment_params)
#
#        # Wait for chaos resources to be ready
#        sleep(3)
#        # Start thread to monitor and reset chaos injection on rescaled flink
#        self.monitor_injection_thread(experiment_params)
#
#    def monitor_injection_thread(self, experiment_params):
#        deployment_name = "flink-taskmanager"
#        # Start chaos injection reset thread
#        self.__log.info(
#            "Starting monitoring thread on scaling events. Reset chaos injection on rescale."
#        )
#        reset_thread = threading.Thread(
#            target=self.__reset_latency, args=(deployment_name, experiment_params)
#        )
#        reset_thread.start()
#
#    # Workaround to reset the latency experiment on rescale as the NetworkChaos resource does not support dynamic updates on target pods
#    def __reset_latency(self, deployment_name, experiment_params):
#        # Definition of the NetworkChaos resource on Flink
#        resource_object = self.t.load_resource_definition(
#            "/app/templates/flink-latency.yaml.j2", experiment_params
#        )
#        # Create API instances
#        apps_v1 = client.AppsV1Api()
#        # Watch for changes in the deployment
#        deployment_stream = watch.Watch().stream(
#            apps_v1.list_namespaced_deployment, namespace="flink"
#        )
#        old_replica_count = None
#        for event in deployment_stream:
#            deployment = event["object"]
#            if deployment.metadata.name == deployment_name:
#                if event["type"] == "DELETED":
#                    self.__log.info("Deployment has been deleted. Exiting...")
#                    break
#                new_replica_count = deployment.spec.replicas
#                if new_replica_count != old_replica_count:
#                    self.__log.info(
#                        "Detected replica change. Triggering latency experiment reset."
#                    )
#                    # Delete the NetworkChaos resource
#                    self.api_instance.delete_namespaced_custom_object(
#                        group="chaos-mesh.org",
#                        version="v1alpha1",
#                        namespace="default",
#                        plural="networkchaos",
#                        name="flink-latency",
#                    )
#                    sleep(3)
#
#                    # Recreate the NetworkChaos resource
#                    self.api_instance.create_namespaced_custom_object(
#                        group="chaos-mesh.org",
#                        version="v1alpha1",
#                        namespace="default",
#                        plural="networkchaos",
#                        body=resource_object,
#                    )
#                    old_replica_count = new_replica_count
#
#    # Delete all networkchaos resources
#    def delete_networkchaos(self):
#        try:
#            self.api_instance.delete_collection_namespaced_custom_object(
#                group="chaos-mesh.org",
#                version="v1alpha1",
#                namespace="default",
#                plural="networkchaos",
#            )
#        except ApiException as e:
#            self.__log.error(
#                f"Exception when calling CustomObjectsApi->delete_collection_namespaced_custom_object: {e}\n"
#            )
#            return
#        self.__log.info("NetworkChaos resources deleted.")
#
#    # Load resource definition template from file and fill in the parameters
#    def create_networkchaos(self, template_filename, experiment_params):
#        # Load resource definition from file
#        resource_object = self.t.load_resource_definition(
#            template_filename, experiment_params
#        )
#        try:
#            self.api_instance.create_namespaced_custom_object(
#                group="chaos-mesh.org",
#                version="v1alpha1",
#                namespace="default",
#                plural="networkchaos",
#                body=resource_object,
#            )
#        except ApiException as e:
#            self.__log.error(
#                f"Exception when calling CustomObjectsApi->create_namespaced_custom_object: {e}\n"
#            )
#            return
#        self.__log.info("NetworkChaos resource created.")
#
#    # Get pods impacted by networkchaos
#    def get_networkchaos_instances(self):
#        try:
#            network_chaos_object = self.api_instance.get_namespaced_custom_object(
#                group="chaos-mesh.org",
#                version="v1alpha1",
#                namespace="default",
#                plural="networkchaos",
#                name="consul-latency",
#            )
#
#            result = [
#                instance.split("/")[1]
#                for instance in network_chaos_object["status"]["instances"]
#            ]
#            return result
#        except ApiException as e:
#            self.__log.error(
#                f"Exception when calling CustomObjectsApi->list_namespaced_custom_object: {e}\n"
#            )
#            return
#
#    # Get node names of impacted consul pods
#    def get_impacted_nodes(self):
#
#        instances = self.get_networkchaos_instances()
#        node_names = []
#
#        for instance in instances:
#            node_names.append(
#                self.node_manager.get_node_by_pod_name(instance, "consul")
#            )
#
#        return node_names
