import os

from datetime import datetime
import paho.mqtt.client as mqtt


from .utils.Logger import Logger
from .utils.ExperimentsData import ExperimentData
from .utils.Config import Config
from .utils.KubernetesManager import KubernetesManager
from .utils.Defaults import DefaultKeys as Key

# Objective
# 1. Start a mqtt server to receive experiment requests
# 2. When "START" request is received, with correct config file, get start timestamp and create a folder with the timestamp as the name
# 2.1. Save the config file in the folder
# 2.2. If chaos is enabled, reset flink taskmanager distribution
# 2.3. Start latency_reset_thread
# 2.4. Deploy transscale-job
# 3. When an experiment finishes or "STOP" request is received, get end timestamp
# 4. Export metrics from victoriametrics start and end timestamp
# 4.1. Evaluate mean throughput for specified time series
# 5. Save transscale-job logs
# 5.1. Export predictions from transscale-job logs
# 5.2. Join predictions to mean throughput
# 6. Delete transscale-job, chaos, load-generators resources from kubernetes cluster
# 6. Restart Flink

EXPERIMENTS_BASE_PATH = "/experiment-volume"


class ExperimentsManager:
    def __init__(self, log: Logger):
        self.__log = log
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.k: KubernetesManager = KubernetesManager(log)
        self.config: Config

    def on_connect(self, client, userdata, flags, rc):
        self.__log.info(f"Connected with result code {rc}")
        self.client.subscribe("experiment/#")

    def on_message(self, client, userdata, msg):
        if msg.topic == "experiment/start":
            self.config = Config(self.__log, msg.payload)
            self.on_message_start()
        elif msg.topic == "experiment/stop":
            self.on_message_stop()

    def start_mqtt_server(self):
        # define address for broker in local kubernetes cluster
        broker = "mqtt-broker.svc.cluster.local"
        self.client.connect(broker, 1883, 60)
        self.client.loop_forever()

    def on_message_start(self):
        # Check that another experiment is not running
        # TODO: check if another experiment is running
        if False:
            self.__log.warning("Another experiment is already running.")
            return
        self.start_experiment()

        # If the experiment succeeds, end it
        self.end_experiment()

    def on_message_stop(self):
        # Check that an experiment is running
        # TODO: check if an experiment is running
        if True:
            # Trigger the end of the experiment
            self.end_experiment()

    def create_exp_folder(self, date):
        # Create the base folder path
        base_folder_path = os.path.join(EXPERIMENTS_BASE_PATH, date)
        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1

        # Create the subfolder
        os.makedirs(subfolder_path)

        # Return the path to the new subfolder
        return subfolder_path

    def create_log_file(self):
        # Create log file
        log_file_path = os.path.join(self.exp_path, "exp_log.txt")

        # Dump experiment information to log file
        with open(log_file_path, "w") as file:
            file.write(f"\nExperiment start at : {self.start_ts}\n")
        return log_file_path

    def start_experiment(self):
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        # Create experiment folder for results
        self.exp_path = self.create_exp_folder(
            datetime.fromtimestamp(self.start_ts).strftime("%m-%d-%Y")
        )

        # Create log file with start timestamp
        self.log_file = self.create_log_file()

        # Dump experiment information to log file
        self.config.save_config(self.exp_path)

        # Check if chaos is enabled
        if self.config.get_bool(Key.Experiment.Chaos.enable):
            self.__log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )
            # Start chaos injection reset thread
            self.__log.info(
                "Starting monitoring thread on scaling events. Reset chaos injection on rescale."
            )
            self.k.monitor_injection_thread()

            # Reset nodes labels
            self.__log.info("Resetting nodes labels.")
            self.k.reset_autoscaling_labels()

            # Reset taskmanager replicas
            self.__log.info("Resetting taskmanager replicas.")
            # Reset to 0 and back to 1 to trigger placement of taskmanager on different relabelled nodes
            self.k.scale_deployment("flink-taskmanager", replicas=0)
            self.k.scale_deployment("flink-taskmanager", replicas=1)

        # Get name of job file to run
        job_file = self.config.get_str(Key.Experiment.job_file)
        self.k.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink run -d -j /tmp/jobs/{job_file}",
        )

        # TODO: deploy load-generators

        # TODO: deploy transscale-job

        # TODO: blocking-loop, wait for transscale-job to finish and retrieve its logs

    def end_experiment(self):
        # Get finish timestamp
        self.end_ts = int(datetime.now().timestamp())

        # Dump information to log file
        with open(self.log_file, "a") as file:
            file.write(f"Experiment end at : {self.end_ts}\n")

        data: ExperimentData = ExperimentData(log=self.__log, exp_path=self.exp_path)

        # Save exported data from VM to file
        data.export_experiment_data()

        if self.config.get_bool(Key.Experiment.output_stats):
            stats, _ = data.eval_stats(
                self.config.get_int(Key.Experiment.output_skip_s)
            )
            if self.config.get_bool(Key.Experiment.output_plot):
                data.eval_plot(stats)
        # Clean flink jobs
        self.k.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="for job_id in $(flink list -r | awk -F ' : ' ' {print $2}'); do flink cancel $job_id ;done",
        )

        # Scale down taskmanagers
        self.k.scale_deployment("flink-taskmanager")
        # Clean transscale job
        self.k.delete_job("transscale-job")
        self.k.delete_pods_by_label("job-name=transscale-job")
        # Delete load generators
        self.k.delete_pods_by_label("app=load-generator")

    def run(self):
        # Start mqtt server
        self.start_mqtt_server()


def main():

    # Create logger
    log = Logger()

    # Create experiment manager
    exp_manager = ExperimentsManager(log)

    # Manage experiments
    exp_manager.run()
