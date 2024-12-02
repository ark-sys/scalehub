from time import sleep

from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class FlinkManager:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k = KubernetesManager(log)

    def get_job_id(self):
        try:
            retry = 3
            while retry > 0:
                job_id = self.k.pod_manager.execute_command_on_pod(
                    deployment_name="flink-jobmanager",
                    command="flink list -r 2>/dev/null | grep RUNNING | awk '{print $4}'",
                ).strip()
                if job_id:
                    self.__log.info(f"[FLK_MGR] Job id: {job_id}")
                    return job_id

                retry -= 1
                sleep(5)
            return None
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting job id: {e}")
            return None

    def get_job_plan(self, job_id):
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

    def get_operator_names(self, job_plan):
        try:
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
            return operator_names
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting operator names: {e}")
            return None

    def get_operator_parallelism(self, job_plan):
        try:
            operator_parallelism = {}
            for node in job_plan["plan"]["nodes"]:
                operator_name = (
                    node["description"]
                    .replace("</br>", "")
                    .replace("<br/>", "")
                    .replace(":", "_")
                    .replace(" ", "_")
                )
                operator_parallelism[operator_name] = node["parallelism"]
            return operator_parallelism
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while getting operator parallelism: {e}")
            return None

    def stop_job(self):
        try:
            job_id = self.get_job_id()
            job_plan = self.get_job_plan(job_id)
            operator_names = self.get_operator_names(job_plan)

            if job_id is None or operator_names is None:
                self.__log.error("[FLK_MGR] Job id or operator names not found.")
                return None, None, None
            else:
                retries = 10
                savepoint_path = None
                sleep_time = 3
                while retries > 0 and savepoint_path is None:
                    resp = self.k.pod_manager.execute_command_on_pod(
                        deployment_name="flink-jobmanager",
                        command=f"flink stop -p -d {job_id}",
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
                    return None, None, None
                sleep(5)
                return job_id, operator_names, savepoint_path
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while stopping job: {e}")
            return None, None, None

    def run_job(self):
        try:
            # Get name of job file to run
            job_file = self.config.get_str(Key.Experiment.job_file)
            self.k.pod_manager.execute_command_on_pod(
                deployment_name="flink-jobmanager",
                command=f"flink run -d -j /tmp/jobs/{job_file}",
            )
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while running job: {e}")

    def scale_job_with_savepoint(self, operator_names, savepoint_path, new_parallelism):
        try:
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
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while rescaling job: {e}")

    def rescale_job(self, new_parallelism):
        try:
            job_id, operator_names, savepoint_path = self.stop_job()
            if savepoint_path is None:
                self.__log.error("[FLK_MGR] Savepoint failed.")
                return
            sleep(10)
            return self.scale_job_with_savepoint(
                operator_names, savepoint_path, new_parallelism
            )
        except Exception as e:
            self.__log.error(f"[FLK_MGR] Error while rescaling job: {e}")
