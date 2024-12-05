import json
import os

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from scripts.monitor.experiments.ExperimentFSM import ExperimentFSM
from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class MQTTClient:
    def __init__(self, log: Logger):
        self.__log = log

        # Create state machine
        self.fsm = ExperimentFSM(log)

        self.fsm.update_state_callback = self.update_state

        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, connect_flags, reason_code, properties):
        self.__log.info(f"[CLIENT] Connected with result code {connect_flags}")

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
            self.__log.info(f"[CLIENT] Received payload {msg.payload}.")
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
                    self.__log.info(f"[CLIENT] Received config: {config}")

                    # Format config as json
                    config = Config(self.__log, json.loads(config))
                    self.fsm._set_config(config)

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
                self.__log.warning(
                    f"[CLIENT] Received non-command payload: {msg.payload}."
                )
        else:
            self.__log.warning(f"[CLIENT] Received invalid topic {msg.topic}.")

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

    log.info("[MONITOR] Starting experiment manager")
    # Manage experiments
    client.run()


if __name__ == "__main__":
    main()
