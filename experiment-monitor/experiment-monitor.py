import json
import os
import threading

from datetime import datetime
from time import sleep

import paho.mqtt.client as mqtt

from utils.Logger import Logger
from utils.ExperimentsData import ExperimentData
from utils.Config import Config
from utils.KubernetesManager import KubernetesManager
from utils.Defaults import DefaultKeys as Key

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


class ExperimentsManager:
    EXPERIMENTS_BASE_PATH = "/experiment-volume"

    def __init__(self, log: Logger):
        self.__log = log
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.k: KubernetesManager = KubernetesManager(log)

        self.config: Config

        self.experiment_count = 0
        self.running_experiment = threading.Event()

    def extract_config(self, msg):
        try:
            # Extract config from message
            config = msg.payload.decode("utf-8")

            self.__log.info(f"Received config: {config}")

            # Format config as json
            self.config = Config(self.__log, json.loads(config))
        except Exception as e:
            self.__log.error(f"Failed to extract config from message: {e}")

    def create_exp_folder(self, date):
        # Create the base folder path
        base_folder_path = os.path.join(self.EXPERIMENTS_BASE_PATH, date)
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
            file.write(f"[CONFIG]\n")
            file.write(f"{self.config.to_str()}\n\n")
            file.write(f"[TIMESTAMPS]\n")
            file.write(f"Experiment start at : {self.start_ts}\n")
            file.write(f"Experiment end at : {self.end_ts}\n")
        return log_file_path

    def on_connect(self, client, userdata, flags, rc):
        self.__log.info(f"Connected with result code {rc}")

        # Subscribe to experiment topics
        self.client.subscribe("experiment/#", qos=2)

    def on_message(self, client, userdata, msg):
        self.__log.info(f"Received message on topic {msg.topic}")
        # Handle start and stop based on topic
        if msg.topic == "experiment/start":
            self.extract_config(msg)
            self.on_message_start()
        elif msg.topic == "experiment/stop" and msg.payload.decode("utf-8") == "STOP":
            self.on_message_stop()

    def start_mqtt_server(self):
        # Get broker host from environment variable
        broker = os.environ.get("MQTT_BROKER_HOST")
        port = os.environ.get("MQTT_BROKER_PORT")

        # Connect to broker
        self.client.connect(broker, int(port), 60)

        # Start mqtt in a blocking loop to keep process alive
        self.client.loop_forever()

    def on_message_start(self):
        self.__log.info("Received start message")
        # Check that another experiment is not running
        if self.experiment_count != 0:
            self.__log.warning("Another experiment is already running.")
            return
        else:
            # Start the experiment in a new thread
            threading.Thread(target=self.start_experiment).start()

    def on_message_stop(self):
        self.__log.info("Received stop message")
        # Check that an experiment is running
        if self.experiment_count == 0:
            self.__log.warning("No experiment is running.")
            return
        else:
            # Stop the experiment
            self.running_experiment.clear()

    def start_experiment(self):
        self.__log.info("Starting experiment")
        # Increment experiment counter
        self.experiment_count += 1

        # Set running experiment flag
        self.running_experiment.set()
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        # Check if chaos is enabled
        if self.config.get_bool(Key.Experiment.Chaos.enable):
            self.__log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )

            # Start chaos injection reset thread
            self.__log.info(
                "Starting monitoring thread on scaling events. Reset chaos injection on rescale."
            )

            # Setup experiment_params
            experiment_params = {
                "latency": self.config.get_int(Key.Experiment.Chaos.delay_latency_ms),
                "jitter": self.config.get_int(Key.Experiment.Chaos.delay_jitter_ms),
                "correlation": self.config.get_float(
                    Key.Experiment.Chaos.delay_correlation
                ),
            }

            self.k.monitor_injection_thread(experiment_params=experiment_params)

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

        # Deploy flink job
        self.k.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command=f"flink run -d -j /tmp/jobs/{job_file}",
        )

        # Resume execution of load generators
        self.k.execute_command_on_pods_by_label(
            "app=load-generator", command="touch /start_generation"
        )

        # Retrieve resource definition for transscale-job
        transscale_resource_definition = self.k.get_configmap("transscale-job")

        # Run transscale-job
        self.k.create_job(transscale_resource_definition)

        # Send ack message to start topic
        self.client.publish("ack/experiment/start", "RUNNING", retain=True, qos=2)
        while self.running_experiment.is_set():
            sleep(1)
            self.__log.info("Waiting for experiment to finish or stop message.")

            job_status = self.k.get_job_status("transscale-job")
            if job_status == "Succeeded":
                self.__log.info("Experiment finished.")
                self.running_experiment.clear()

        self.__log.info("Experiment finished or stopped.")
        # Send ack message to stop topic
        self.client.publish("ack/experiment/stop", "STOPPING", retain=True, qos=2)

        self.end_ts = int(datetime.now().timestamp())

        # Create experiment folder for results, ordered by date (YYYY-MM-DD)
        self.exp_path = self.create_exp_folder(
            datetime.fromtimestamp(self.start_ts).strftime("%Y-%m-%d")
        )

        # Create log file with start timestamp
        self.log_file = self.create_log_file()

        # End experiment
        self.end_experiment()

    def end_experiment(self):
        self.__log.info("Ending experiment")

        data: ExperimentData = ExperimentData(
            log=self.__log, exp_path=self.exp_path, config=self.config
        )

        # Export data from victoriametrics
        data.export_experiment_data()

        if self.config.get_bool(Key.Experiment.output_stats):
            # If output_stats is enabled, evaluate mean throughput and extract predictions from transscale-job logs in stats.csv file
            stats, _ = data.eval_stats(
                self.config.get_int(Key.Experiment.output_skip_s)
            )
            # If output_plot is enabled, evaluate plot from stats.csv file
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
        # Clean transscale remaining pods
        self.k.delete_pods_by_label("job-name=transscale-job")
        # Delete load generators
        self.k.delete_pods_by_label("app=load-generator")
        if self.config.get_bool(Key.Experiment.Chaos.enable):
            # Clean all network chaos resources
            self.k.delete_networkchaos()

        # Reset counter
        self.experiment_count -= 1

    def run(self):
        # Start mqtt server
        self.start_mqtt_server()


def main():

    # Create logger
    log = Logger()

    # Create experiment manager
    exp_manager = ExperimentsManager(log)

    log.info("Starting experiment manager")
    # Manage experiments
    exp_manager.run()


if __name__ == "__main__":
    main()
