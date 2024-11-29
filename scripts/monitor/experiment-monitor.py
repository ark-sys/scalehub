import json
import os
import threading

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from transitions import Machine

from scripts.monitor.experiments.SimpleExperiment import SimpleExperiment
from scripts.monitor.experiments.StandaloneExperiment import StandaloneExperiment
from scripts.monitor.experiments.TransscaleExperiment import TransscaleExperiment
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class ExperimentFSM:
    # Define states of the state machine
    states = ["IDLE", "STARTING", "RUNNING", "FINISHING"]

    def __init__(self, log: Logger):
        self.__log = log

        # This holds the current experiment instance
        self.current_experiment = None
        self.experiment_event = threading.Event()

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

    def set_config(self, config):
        self.__log.info("Setting config.")
        self.config = config

    def create_experiment_instance(self, experiment_type):
        match experiment_type:
            case "standalone":
                return StandaloneExperiment(self.__log, self.config)
            case "transscale":
                return TransscaleExperiment(self.__log, self.config)
            case "simple":
                return SimpleExperiment(self.__log, self.config)
            case _:
                raise ValueError(f"Invalid experiment type: {experiment_type}")

    def start_experiment(self):
        experiment_type = self.config.get_str(Key.Experiment.type)
        self.__log.info(f"Starting experiment: {experiment_type}")

        self.__log.info(f"State is {self.state}")

        try:
            # Create experiment instance with current config
            self.current_experiment = self.create_experiment_instance(experiment_type)
            self.current_experiment.start()
            self.__log.info("Experiment started.")
            # Trigger run transition
            self.run()
        except Exception as e:
            self.__log.error(f"Error while starting experiment: {e}")
            self.__log.error(f"Cleaning experiment.")
            self.clean()

    def run_experiment(self):
        self.__log.info("Running experiment.")

        def thread_wrapper():
            try:
                self.current_experiment.running()
            finally:
                # Trigger finish transition
                self.experiment_event.set()
                if self.is_RUNNING():
                    self.finish()

        # Create thread, to run experiment in background
        experiment_thread = threading.Thread(target=thread_wrapper)
        experiment_thread.start()

    def end_experiment(self):
        self.__log.info("Experiment finished or stopped.")
        if self.current_experiment:
            self.current_experiment.stop()
            self.clean()

    def clean_experiment(self):
        # Clean flink jobs
        self.__log.info("Cleaning experiment.")
        self.current_experiment.cleanup()
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


def test_fsm():
    log = Logger()
    path = "/app/conf/experiment/multiple_run.ini"
    config = Config(log, path)
    fsm = ExperimentFSM(log)
    fsm.set_config(config)

    fsm.start()
    # fsm.run()
    # fsm.finish()
    # fsm.clean()


if __name__ == "__main__":
    main()

    # test_fsm()
