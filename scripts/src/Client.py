import json
import time
from time import sleep

import paho.mqtt.client as mqtt
# noinspection PyUnresolvedReferences
from paho.mqtt.enums import CallbackAPIVersion

from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger
from scripts.utils.Tools import Playbooks


# 1. Create MQTT client
# 2. Connect to MQTT broker
# 3. Publish "start" or "stop" message on experiment topic
class Client:
    def __init__(self, log: Logger, configs: list[Config]):
        self.__log = log
        self.p: Playbooks = Playbooks(log)

        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION1)
        self.configs = configs
        self.broker_host = configs[0].get_str(Key.Experiment.broker_mqtt_host.key)
        self.broker_port = configs[0].get_int(Key.Experiment.broker_mqtt_port.key)
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
            self.__log.error(f"MQTT connection failed: {str(e)}")
            exit(1)
        self.client.loop_start()

    def start(self):
        self.__log.info("Starting experiment")
        # Generate payload
        configs_json = [config.to_json() for config in self.configs]
        # Serialize payload
        ser_payload = json.dumps(configs_json)
        payload = {"command": "START", "configs": ser_payload}
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
            time.sleep(1)
        if self.ack == "INVALID_COMMAND":
            self.__log.error(f"Command START failed. State is {self.state}")
            exit(1)

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
            time.sleep(1)
        if self.ack == "INVALID_COMMAND":
            self.__log.error(f"Command STOP failed. State is {self.state}")
            exit(1)

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

    # Check last experiment state
    def check(self):
        sleep(3)
        self.__log.info(f"Current experiment state is {self.state}.")
