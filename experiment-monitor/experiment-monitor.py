import json
import os
import threading
from datetime import datetime
from time import sleep

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from transitions import Machine

from utils.Config import Config
from utils.Defaults import DefaultKeys as Key
from utils.ExperimentsData import ExperimentData
from utils.KubernetesManager import KubernetesManager
from utils.Logger import Logger


# Objective
# 1. Start a mqtt server to receive experiment requests
# 2. When "START" request is received, with correct config file, load config file
# 2.1. Save the config file in the exp folder and get start timestamp
# 2.2. If chaos is enabled, reset flink taskmanager distribution
# 2.3. Start chaos
# 2.4. Start latency_reset_thread (networkchaos resources aren't applied on new flink instances when they are rescaled)
# 2.5. Deploy load generators
# 2.6. Start Flink job
# 2.7. Deploy transscale-job
# 3. When an experiment finishes or "STOP" request is received, get end timestamp
# 4. Export metrics from victoriametrics with start and end timestamp
# 4.1. Evaluate mean throughput for specified time series
# 5. Save transscale-job logs
# 5.1. Export predictions from transscale-job logs
# 5.2. Join predictions to mean throughput
# 6. If CLEAN request is received or experiment finishes, clean experiment
# 6.1. Clean flink jobs
# 6.2. Scale down taskmanagers
# 6.3. Clean transscale job
# 6.4. Clean transscale remaining pods
# 6.5. Delete load generators
# 6.6. Clean chaos resources


class ExperimentFSM:
    # Define states of the state machine
    states = ["IDLE", "STARTING", "RUNNING", "FINISHING"]
    EXPERIMENTS_BASE_PATH = "/experiment-volume"
    TEMPLATES_BASE_PATH = "/app/templates"

    def __init__(self, log: Logger):
        self.__log = log
        self.k: KubernetesManager = KubernetesManager(log)

        self.config = None
        self.start_ts = None
        self.end_ts = None
        self.exp_path = None

        # Initialize state machine
        self.machine = Machine(model=self, states=ExperimentFSM.states, initial="IDLE")

        # Add transitions
        self.machine.add_transition(
            trigger="start", source="IDLE", dest="STARTING", after="start_experiment"
        )
        self.machine.add_transition(
            trigger="run", source="STARTING", dest="RUNNING", after="run_experiment"
        )
        self.machine.add_transition(
            trigger="finish", source="RUNNING", dest="FINISHING", after="end_experiment"
        )
        self.machine.add_transition(
            trigger="clean", source="*", dest="IDLE", after="clean_experiment"
        )

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

    def set_config(self, config):
        self.config = config

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
        try:
            # Create the subfolder
            os.makedirs(subfolder_path)
        except OSError as e:
            self.__log.error(
                f"Error while creating experiment folder {subfolder_path}: {e}"
            )
            raise e
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

    def start_experiment(self):
        self.__log.info("Starting experiment")

        self.__log.info(f"State is {self.state}")
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        try:
            # Check if chaos is enabled
            if self.config.get_bool(Key.Experiment.Chaos.enable):
                self.__log.info(
                    "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
                )

                # Start chaos injection reset thread
                self.__log.info(
                    "Starting monitoring thread on scaling events. Reset chaos injection on rescale."
                )

                # Remove label 'chaos=true' from all nodes
                chaos_label = "chaos=true"
                worker_nodes = self.k.get_nodes(
                    "node-role.kubernetes.io/worker=consumer"
                )
                self.k.remove_label_from_nodes(worker_nodes, chaos_label)

                # Setup experiment_params
                chaos_params = {
                    "latency": self.config.get_int(
                        Key.Experiment.Chaos.delay_latency_ms
                    ),
                    "jitter": self.config.get_int(Key.Experiment.Chaos.delay_jitter_ms),
                    "correlation": self.config.get_float(
                        Key.Experiment.Chaos.delay_correlation
                    ),
                    "rate": self.config.get_int(
                        Key.Experiment.Chaos.bandwidth_rate_mbps
                    ),
                    "limit": self.config.get_int(Key.Experiment.Chaos.bandwidth_limit),
                    "buffer": self.config.get_int(
                        Key.Experiment.Chaos.bandwidth_buffer
                    ),
                }

                # Deploy chaos resources
                self.k.create_networkchaos(self.consul_chaos_template, chaos_params)

                # Wait for chaos on consul pods to be ready
                sleep(3)
                # Label nodes hosting an impacted consul pod with 'chaos=true'
                impacted_nodes = self.k.get_impacted_nodes()
                self.k.add_label_to_nodes(impacted_nodes, chaos_label)

                # Deploy chaos resources on Flink and Storage instances running on chaos nodes
                self.k.create_networkchaos(self.flink_chaos_template, chaos_params)
                # self.k.create_networkchaos(self.storage_chaos_template, chaos_params)

                # Wait for chaos resources to be ready
                sleep(3)

                # Start thread to monitor and reset chaos injection on rescaled flink
                self.k.monitor_injection_thread(experiment_params=chaos_params)

                # Reset nodes labels
                self.__log.info("Resetting nodes labels.")

                # remove label "node-role.kubernetes.io/autoscaling" from all nodes
                autoscaling_label = "node-role.kubernetes.io/autoscaling"
                self.k.remove_label_from_nodes(
                    worker_nodes, f"{autoscaling_label}=SCHEDULABLE"
                )
                self.k.remove_label_from_nodes(
                    worker_nodes, f"{autoscaling_label}=UNSCHEDULABLE"
                )
                # Get clean nodes (worker_nodes - impacted_nodes)
                clean_nodes = list(set(worker_nodes) - set(impacted_nodes))
                # Add label "node-role.kubernetes.io/autoscaling" to clean nodes
                self.k.add_label_to_nodes(
                    clean_nodes, f"{autoscaling_label}=SCHEDULABLE"
                )

                # Reset taskmanager replicas
                self.__log.info("Resetting taskmanager replicas.")
                # Reset to 0 and back to 1 to trigger placement of taskmanager on schedulable nodes
                self.k.scale_deployment("flink-taskmanager", replicas=0)
                sleep(1)
                self.k.scale_deployment("flink-taskmanager", replicas=1)

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
                self.k.create_service(
                    self.load_generator_service_template, load_generator_params
                )
                self.k.create_deployment(
                    self.load_generator_deployment_template, load_generator_params
                )

            # Get name of job file to run
            job_file = self.config.get_str(Key.Experiment.job_file)

            # Deploy flink job
            self.k.execute_command_on_pod(
                deployment_name="flink-jobmanager",
                command=f"flink run -d -j /tmp/jobs/{job_file}",
            )

            # # Resume execution of load generators
            # self.k.execute_command_on_pods_by_label(
            #     "type=load-generator", command="touch /start_generation"
            # )

            # Retrieve resource definition for transscale-job
            transscale_resource_definition = self.k.get_configmap(
                "transscale-job-definition"
            )

            # Run transscale-job
            self.k.create_job(transscale_resource_definition["transscale-job.yaml"])

            self.__log.info("Experiment started.")

            # Trigger run transition
            self.run()
        except Exception as e:
            self.__log.error(f"Error while starting experiment: {e}")
            self.__log.error(f"Cleaning experiment.")
            self.clean()

    def run_experiment(self):
        self.__log.info("Running experiment.")
        # Create thread, to run experiment in background
        experiment_thread = threading.Thread(target=self.running_experiment)

        experiment_thread.start()

    def running_experiment(self):
        # Wait for experiment to finish or stop message
        while self.is_RUNNING():
            # self.__log.info("Waiting for experiment to finish or stop message.")
            sleep(1)
            try:
                job_status = self.k.get_job_status("transscale-job")
                # Check if job_status has any element
                if not job_status:
                    continue
                else:
                    if job_status[0].type == "Complete":
                        self.__log.info("Experiment finished.")
                        break
            except Exception as e:
                self.__log.warning(f"Error while getting job status: {e}")

        # if arrived here after the loop break, then trigger finish transition
        if self.is_RUNNING():
            self.finish()

    def end_experiment(self):
        self.__log.info("Experiment finished or stopped.")
        self.end_ts = int(datetime.now().timestamp())
        try:
            # Create experiment folder for results, ordered by date (YYYY-MM-DD)
            self.exp_path = self.create_exp_folder(
                datetime.fromtimestamp(self.start_ts).strftime("%Y-%m-%d")
            )

            # Create log file with start timestamp
            self.log_file = self.create_log_file()

            # Get logs from transscale-job
            transscale_logs = self.k.get_job_logs("transscale-job", "default")

            # Save transscale-job logs
            with open(os.path.join(self.exp_path, "transscale_log.txt"), "w") as file:
                file.write(transscale_logs)

            # Export experiment data
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
                    data.eval_everything()

            self.__log.info("Experiment ended.")
        except Exception as e:
            self.__log.error(f"Error while ending experiment: {e}")
            self.__log.error(f"Cleaning experiment.")
        finally:
            # Trigger clean transition
            self.clean()

    def clean_experiment(self):
        # Clean flink jobs
        self.__log.info("Cleaning experiment.")
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
        for generator in self.config.get(Key.Experiment.Generators.generators):
            load_generator_params = {
                "lg_name": generator["name"],
                "lg_topic": generator["topic"],
                "lg_numsensors": int(generator["num_sensors"]),
                "lg_intervalms": int(generator["interval_ms"]),
                "lg_replicas": int(generator["replicas"]),
                "lg_value": int(generator["value"]),
            }
            self.k.delete_service(
                self.load_generator_service_template, load_generator_params
            )
            self.k.delete_deployment(
                self.load_generator_deployment_template, load_generator_params
            )

        if self.config.get_bool(Key.Experiment.Chaos.enable):
            # Clean all network chaos resources
            self.k.delete_networkchaos()

        # Clean variables
        self.start_ts = None
        self.end_ts = None
        self.exp_path = None
        self.config = None

        self.__log.info("Cleaning complete.")
        self.__log.info(f"Returning to state IDLE")


class MQTTClient:
    def __init__(self, log: Logger):
        self.__log = log

        # Create state machine
        self.fsm = ExperimentFSM(log)

        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, connect_flags, reason_code, properties):
        self.__log.info(f"Connected with result code {connect_flags}")

        # Subscribe to experiment command topic
        self.client.subscribe("experiment/command", qos=2)

        # Publish current fsm state
        self.update_state(self.fsm.state)

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True

    def on_message(self, client, userdata, msg):
        if msg.topic == "experiment/command":
            # Check if payload is in json format
            self.__log.info(f"Received payload {msg.payload}.")
            if self.is_json(msg.payload.decode("utf-8")):
                payload = json.loads(msg.payload.decode("utf-8"))
                command = payload.get("command")

                if command == "STOP" and self.fsm.is_RUNNING():
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_STOP", retain=True, qos=2
                    )

                    # Trigger finish transition
                    self.fsm.finish()

                    # Publish current fsm state
                    self.update_state(self.fsm.state)

                elif command == "START" and self.fsm.is_IDLE():
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_START", retain=True, qos=2
                    )

                    config = payload.get("config")
                    self.__log.info(f"Received config: {config}")

                    # Format config as json
                    config = Config(self.__log, json.loads(config))
                    self.fsm.set_config(config)

                    # Trigger start transition
                    self.fsm.start()

                    # Publish current fsm state
                    self.update_state(self.fsm.state)

                elif command == "CLEAN":
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_CLEAN", retain=True, qos=2
                    )

                    # Trigger clean transition
                    self.fsm.clean()

                    # Publish current fsm state
                    self.update_state(self.fsm.state)

                else:
                    self.__log.warning(
                        f"Received invalid command {command} for state {self.fsm.state}."
                    )
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Publish current fsm state
                    self.update_state(self.fsm.state)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "INVALID_COMMAND", retain=True, qos=2
                    )
            # Ignore message with empty payload
            elif msg.payload == b"":
                pass
            else:
                self.__log.warning(f"Received non-command payload: {msg.payload}.")
        else:
            self.__log.warning(f"Received invalid topic {msg.topic}.")

    def update_state(self, state):
        # Send state message
        self.client.publish("experiment/state", state, retain=True, qos=2)

    def start_mqtt_client(self):
        # Get broker info from environment variable
        broker = os.environ.get("MQTT_BROKER_HOST")
        port = os.environ.get("MQTT_BROKER_PORT")
        username = os.environ.get("MQTT_BROKER_USERNAME")
        password = os.environ.get("MQTT_BROKER_PASSWORD")

        # Set username and password
        self.client.username_pw_set(username, password=password)

        # Connect to broker
        self.client.connect(broker, int(port), 60)

        # Start mqtt in a blocking loop to keep process alive
        self.client.loop_forever()

    def run(self):
        # Start mqtt server
        self.start_mqtt_client()


def main():
    # Create logger
    log = Logger()

    # Create client
    client = MQTTClient(log)

    log.info("Starting experiment manager")
    # Manage experiments
    client.run()


if __name__ == "__main__":
    main()
