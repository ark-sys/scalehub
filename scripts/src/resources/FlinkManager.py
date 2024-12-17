import re
from time import sleep

from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class FlinkManager:
    def __init__(self, log: Logger, config: Config, km: KubernetesManager):
        self.__log = log
        self.config = config
        self.k = km
        self.flink_host = "flink-jobmanager.flink.svc.cluster.local"
        self.flink_port = 8081

        # Store running job information
        self.monitored_task = self.config.get_str(Key.Experiment.task_name)
        self.job_file = self.config.get_str(Key.Experiment.job_file)
        self.job_plan = None
        self.job_id = None
        self.operators = {}
        self.monitored_task_parallelism = None
        self.savepoint_path = None

    def __get_overview(self):
        try:
            import requests

            r = requests.get(f"http://{self.flink_host}:{self.flink_port}/overview")
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting overview: {e}")
            return None

    def __get_job_plan(self, job_id):
        try:
            import requests

            retry = 3
            while retry > 0:
                r = requests.get(
                    f"http://flink-jobmanager.flink.svc.cluster.local:8081/jobs/{job_id}/plan"
                )
                if r.status_code == 200:
                    self.__log.info(f"[FLK_MGR] Job plan response: {r.text}")
                    return r.json()
                retry -= 1
                sleep(5)
            return None
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting job plan: {e}")
            return None

    def __get_job_state(self):
        # retrieve status of the job
        try:
            import requests

            r = requests.get(
                f"http://flink-jobmanager.flink.svc.cluster.local:8081/jobs/{self.job_id}/status"
            )
            if r.status_code == 200:
                return r.json()["status"]
            else:
                return None
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting job state: {e}")
            return None

    def __get_operators(self):
        # Build dictionary with operator names as keys and parallelism as values
        try:
            operator_names = {}
            for node in self.job_plan["plan"]["nodes"]:
                operator_name = (
                    node["description"]
                    .replace("</br>", "")
                    .replace("<br/>", "")
                    .replace(":", "_")
                    .replace(" ", "_")
                )
                operator_names[operator_name] = node["parallelism"]
            return operator_names
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting operator names: {e}")
            return None

    def __get_monitored_task_parallelism(self):
        self.__log.info(f"[FLK_MGR] Current operator names: {self.operators.keys()}")
        for operator in self.operators:
            if self.monitored_task in operator:
                return self.operators[operator]

    def __stop_job(self):
        try:

            if self.job_id is None:
                self.__log.error("[FLK_MGR] Job id not found.")
                return None
            else:
                retries = 10
                savepoint_path = None
                sleep_time = 3
                while retries > 0 and savepoint_path is None:
                    resp = self.k.pod_manager.execute_command_on_pod(
                        deployment_name="flink-jobmanager",
                        command=f"flink stop -p -d {self.job_id}",
                    )
                    for line in resp.split("\n"):
                        if "Savepoint completed." in line:
                            savepoint_path = line.split("Path:")[1].strip()
                            self.__log.info(
                                f"[FLK_MGR] Savepoint path: {savepoint_path}"
                            )
                            break
                    retries -= 1
                    # At each iteration increase sleep time
                    sleep_time += 1
                    sleep(sleep_time)
                if savepoint_path is None:
                    self.__log.error("[FLK_MGR] Savepoint failed.")
                    return None
                sleep(5)
                return savepoint_path
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while stopping job: {e}")
            return None

    def __build_par_map(self, new_parallelism) -> str:

        for operator in self.operators:
            if self.monitored_task in operator:
                self.operators[operator] = new_parallelism

        # Join "operator:parallelism" pairs with ";"
        operators_list = [
            f"{operator}:{self.operators[operator]}" for operator in self.operators
        ]
        return ";".join(operators_list)

    def run_job(self, new_parallelism=None):
        try:
            self.__log.info("[FLK_MGR] Running job.")
            if new_parallelism is not None:
                self.__log.info(f"[FLK_MGR] Rescaling job to {new_parallelism}.")
                self.savepoint_path = self.__stop_job()
                if self.savepoint_path is None:
                    self.__log.error("[FLK_MGR] Savepoint failed.")
                    return 1
                sleep(10)
                par_map = self.__build_par_map(new_parallelism)
                res = self.k.pod_manager.execute_command_on_pod(
                    deployment_name="flink-jobmanager",
                    command=f"flink run -d -s {self.savepoint_path} -j /tmp/jobs/{self.job_file} --parmap '{par_map}'",
                )
                self.__log.info(
                    f"[FLK_MGR] Operator {self.monitored_task} rescaled to {new_parallelism}."
                )
            else:
                # Simply run the job
                res = self.k.pod_manager.execute_command_on_pod(
                    deployment_name="flink-jobmanager",
                    command=f"flink run -d -j /tmp/jobs/{self.job_file}",
                )
            # Extract job id from response
            self.job_id = re.search(r"JobID ([a-f0-9]+)", res).group(1)
            if self.job_id:
                self.__log.info(f"[FLK_MGR] Running job id: {self.job_id}")
            else:  # Job id not found
                self.__log.error("[FLK_MGR] Job id not found.")
                return 1
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while running job: {e}")
            return 1

    def get_total_slots(self):
        overview = self.__get_overview()
        if overview is not None:
            return overview["slots-total"]
        return None

    def get_total_taskmanagers(self):
        overview = self.__get_overview()
        if overview is not None:
            return overview["taskmanagers"]
        return None

    def check_nominal_job_run(self):
        # List current running jobs. If we multiple jobs running beside self.job_id. Cancel all of them
        res = self.k.pod_manager.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="flink list -r 2>/dev/null'",
        ).strip()

        # Extract job ids from response
        job_ids = re.findall(r"\b\w{16,}\b", res)
        for job_id in job_ids:
            # Cancel all jobs except the one tracked by self.job_id
            if job_id != self.job_id:
                self.k.pod_manager.execute_command_on_pod(
                    deployment_name="flink-jobmanager",
                    command=f"flink cancel {job_id}",
                )
                self.__log.info(f"[FLK_MGR] Job {job_id} cancelled.")

        return 0

    def get_job_info(self):
        try:
            self.__log.info("[FLK_MGR] Getting job info.")
            if self.job_id is None:
                self.__log.error("[FLK_MGR] Job id not found.")
                return None
            self.job_plan = self.__get_job_plan(self.job_id)
            self.operators = self.__get_operators()
            self.monitored_task_parallelism = self.__get_monitored_task_parallelism()

            if self.operators is None:
                self.__log.error("[FLK_MGR] Operator names not found.")
                return None
            else:
                return 0
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting job info: {e}")
            return None

    def wait_for_job_running(self):
        job_state = None
        try:
            retries = 10
            while retries > 0 and job_state != "RUNNING":
                job_state = self.__get_job_state()
                if job_state == "RUNNING":
                    return 0
                retries -= 1
                sleep(5)
            if job_state != "RUNNING":
                self.__log.error("[FLK_MGR] Job did not start.")
                return 1
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while waiting for job to run: {e}")
            return 1
