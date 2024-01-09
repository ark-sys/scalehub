import json
import os
import threading
import time
import paho.mqtt.client as mqtt
import enoslib as en

from .utils.Config import Config
from .utils.Logger import Logger
from .utils.Defaults import DefaultKeys as Key

# 1. Create MQTT client
# 2. Connect to MQTT broker
# 3. Publish "start" or "stop" message on experiment topic


class Playbooks:
    # Main call to run a playbook, checks if type is create or delete
    def run_playbook(self, playbook, config: Config, tag=None, extra_vars=None):
        if extra_vars is None:
            extra_vars = {}
        inventory = config.get_str(Key.Scalehub.inventory)
        playbook_filename = f"{config.get_str(Key.Scalehub.playbook)}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")

        playbook_vars = {
            "kubeconfig_path": os.environ["KUBECONFIG"],
            "shub_config": config.to_json(),
        }
        playbook_vars.update(extra_vars)

        match tag:
            case "create":
                arg_tags = ["create"]
            case "delete":
                arg_tags = ["delete"]
            case "experiment":
                arg_tags = ["experiment"]
            case _:
                arg_tags = []

        # Run the playbook with additional tags and extra vars
        en.run_ansible(
            playbooks=[playbook_filename],
            tags=arg_tags,
            extra_vars=playbook_vars,
            inventory_path=inventory,
        )


class Client:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.p: Playbooks = Playbooks()

        self.client = mqtt.Client()
        self.config = config
        self.broker_host = config.get_str(Key.Experiment.broker_mqtt_host)
        self.broker_port = config.get_int(Key.Experiment.broker_mqtt_port)
        self.mqtt_user = "scalehub"
        self.mqtt_pass = "s_password"
        self.ack = None
        self.state = None
        self.init_state = None

        self.setup_mqtt()

    def on_message(self, client, userdata, message):

        match message.topic:
            case "experiment/ack":
                self.ack = message.payload.decode("utf-8")
                self.__log.info(f"Received ack {self.ack}")
            case "experiment/state":
                self.state = message.payload.decode("utf-8")
                self.__log.info(f"Received state {self.state}")

    def on_connect(self, client, userdata, flags, rc):
        self.__log.info(f"Connected with result code {rc}")

        # Subscribe to experiment topics
        self.client.subscribe("experiment/ack", qos=2)
        self.client.subscribe("experiment/state", qos=2)

    def setup_mqtt(self):
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.username_pw_set(self.mqtt_user, self.mqtt_pass)
        try:
            self.client.connect(
                self.broker_host,
                self.broker_port,
                60,
            )
        except ConnectionRefusedError as e:
            self.__log.error(f"MQTT connection failed: {e}")
            exit(1)
        self.client.loop_start()

    def start(self):
        self.__log.info("Starting experiment")

        # Create START payload with experiment config
        payload = {"command": "START", "config": self.config.to_json()}
        # Get string representation of payload
        payload = json.dumps(payload)

        # Send message to remote experiment-monitor to start experiment
        self.client.publish(
            "experiment/command",
            payload=payload,
            qos=2,
            retain=True,
        )
        # Wait message on experiment/start
        self.__log.info("Waiting for ack...")
        while self.ack != "ACK_START":
            if self.ack == "INVALID_COMMAND":
                self.__log.error(f"Command START failed. State is {self.state}")
                exit(1)
            time.sleep(1)
        # Wait for state change
        self.__log.info("Waiting for state change...")
        while self.state != "RUNNING":
            time.sleep(1)
        self.__log.info(f"Experiment state changed to {self.state}.")

    def stop(self):

        # Create STOP payload
        payload = {"command": "STOP"}

        # Get string representation of payload
        payload = json.dumps(payload)

        # Send stop message to remote experiment-monitor
        self.client.publish("experiment/command", payload, qos=2, retain=True)

        # Wait message on experiment/stop
        self.__log.info("Waiting for ack...")
        while self.ack != "ACK_STOP":
            if self.ack == "INVALID_COMMAND":
                self.__log.error(f"Command STOP failed. State is {self.state}")
                exit(1)
            time.sleep(1)
        self.__log.info("Waiting for state change...")
        while self.state != "IDLE":
            time.sleep(1)
        self.__log.info(f"Experiment state changed to {self.state}.")

    def clean(self):
        self.__log.info("Cleaning experiment")

        # Create CLEAN payload
        payload = {"command": "CLEAN"}

        # Get string representation of payload
        payload = json.dumps(payload)

        # Clean messages on experiment/command
        self.client.publish("experiment/command", payload, qos=2, retain=True)

        # Wait message on experiment/stop
        self.__log.info("Waiting for ack...")
        while self.ack != "ACK_CLEAN":
            time.sleep(1)
        self.__log.info("Waiting for state change...")
        while self.state != "IDLE":
            time.sleep(1)
        self.__log.info(f"Experiment state changed to {self.state}.")
