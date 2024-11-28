import os
from time import sleep

from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.KubernetesManager import KubernetesManager
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Tools, Playbooks


class Experiment:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"
    TEMPLATES_BASE_PATH = "/app/templates"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k: KubernetesManager = KubernetesManager(log)
        self.t: Tools = Tools(log)
        self.p: Playbooks = Playbooks(log)

        # Chaos resources templates
        self.consul_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/consul-latency.yaml.j2"
        )
        self.flink_chaos_template = f"{self.TEMPLATES_BASE_PATH}/flink-latency.yaml.j2"
        self.storage_chaos_template = (
            f"{self.TEMPLATES_BASE_PATH}/storage-latency.yaml.j2"
        )
        self.load_generator_deployment_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-deployment.yaml.j2"
        )
        self.load_generator_service_template = (
            f"{self.TEMPLATES_BASE_PATH}/load-generator-service.yaml.j2"
        )

    def create_log_file(self, exp_path, start_ts, end_ts):
        # Create log file
        log_file_path = os.path.join(exp_path, "exp_log.txt")

        # Dump experiment information to log file
        with open(log_file_path, "w") as file:
            file.write(f"[CONFIG]\n")
            file.write(f"{self.config.to_json()}\n\n")
            file.write(f"[TIMESTAMPS]\n")
            file.write(f"Experiment start at : {start_ts}\n")
            file.write(f"Experiment end at : {end_ts}\n")
        return log_file_path

    def run_job(self):
        # Get name of job file to run
        job_file = self.config.get_str(Key.Experiment.job_file)
        self.k.pod_manager.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink run -d -j /tmp/jobs/{job_file}",
        )

    # Prepare scaling when manually initiated
    def stop_job(self) -> (str, list, str):
        # Get current job id
        job_id = self.k.pod_manager.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="flink list -r 2>/dev/null | grep RUNNING | awk '{print $4}'",
        ).strip()

        self.__log.info(f"Job id: {job_id}")

        # Get current job operators list
        import requests

        r = requests.get(
            f"http://flink-jobmanager.flink.svc.cluster.local:8081/jobs/{job_id}/plan"
        )
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

        retries = 5
        savepoint_path = None
        while retries > 0 and savepoint_path is None:
            resp = self.k.pod_manager.execute_command_on_pod(
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

    def rescale_job_from_savepoint(
        self, operator_names, savepoint_path, new_parallelism
    ):
        # Get monitored task
        monitored_task = self.config.get_str(Key.Experiment.task_name)

        # Get name of job file to run
        job_file = self.config.get_str(Key.Experiment.job_file)

        # Build new par_map
        par_map = []
        for operator_name in operator_names:
            if monitored_task in operator_name:
                par_map.append(f"{operator_name}:{new_parallelism}")
            else:
                par_map.append(f"{operator_name}:1")

        # Rescale job from savepoint
        return self.k.pod_manager.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink run -d -s {savepoint_path} -j /tmp/jobs/{job_file} --parmap '{';'.join(par_map)}'",
        )

    def run_load_generators(self):
        # Create load generators
        for generator in self.config.get(Key.Experiment.Generators.generators):
            load_generator_params = {
                "lg_name": generator["name"],
                "lg_topic": generator["topic"],
                "lg_numsensors": int(generator["num_sensors"]),
                "lg_intervalms": int(generator["interval_ms"]),
                "lg_replicas": int(generator["replicas"]),
                "lg_value": int(generator["value"]),
            }
            # self.p.run(
            #     "application/load_generators",
            #     config=self.config,
            #     tag="create",
            #     extra_vars=load_generator_params,
            # )
            self.k.service_manager.create_service(
                self.load_generator_service_template, load_generator_params
            )
            self.k.deployment_manager.create_deployment(
                self.load_generator_deployment_template, load_generator_params
            )

    def delete_load_generators(self):
        # Delete load generators
        for generator in self.config.get(Key.Experiment.Generators.generators):
            load_generator_params = {
                "lg_name": generator["name"],
                "lg_topic": generator["topic"],
                "lg_numsensors": int(generator["num_sensors"]),
                "lg_intervalms": int(generator["interval_ms"]),
                "lg_replicas": int(generator["replicas"]),
                "lg_value": int(generator["value"]),
            }
            self.p.run(
                "application/load_generators",
                config=self.config,
                tag="delete",
                extra_vars=load_generator_params,
            )
            self.k.service_manager.delete_service(
                self.load_generator_service_template, load_generator_params
            )
            self.k.deployment_manager.delete_deployment(
                self.load_generator_deployment_template, load_generator_params
            )

    def is_chaos_enabled(self):
        return self.config.get_bool("chaos.enabled")

    def start(self):
        raise NotImplementedError("Start method not implemented.")

    def stop(self):
        raise NotImplementedError("Stop method not implemented.")

    def cleanup(self):
        raise NotImplementedError("Cleanup method not implemented.")

    def running(self):
        raise NotImplementedError("Running method not implemented.")


class Scaling:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config

    # Check if we reached last step)
