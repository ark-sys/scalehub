import json
import os
import threading

import paho.mqtt.client as mqtt
# noinspection PyUnresolvedReferences
from paho.mqtt.enums import CallbackAPIVersion

from scripts.monitor.experiments.ExperimentFSM import (
    ExperimentFSM,
    FSMThreadWrapper,
    States,
)
from scripts.utils.Config import Config
from scripts.utils.Logger import Logger


class MQTTClient(threading.Thread):
    def __init__(self, log: Logger, fsm_thread: FSMThreadWrapper):
        super().__init__()
        self.__log = log

        # FSM instance
        self.current_fsm_thread = fsm_thread
        self.current_fsm = fsm_thread.get_fsm()

        # Set update state callback in FSM
        self.current_fsm.set_update_state_callback(self.update_state)

        # MQTT client setup
        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, connect_flags, reason_code, properties):
        self.__log.info(f"[CLIENT] Connected with result code {connect_flags}")

        # Subscribe to experiment command topic
        self.client.subscribe("experiment/command", qos=2)

        # Publish current fsm state
        self.update_state()

    @staticmethod
    def is_json(myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError:
            return False
        return True

    def on_message(self, client, userdata, msg):
        if msg.topic == "experiment/command":
            # Check if payload is in json format
            self.__log.info(
                f"[CLIENT] Received payload {msg.payload}. Current state: {self.current_fsm.state}."
            )

            if self.is_json(msg.payload.decode("utf-8")):
                payload = json.loads(msg.payload.decode("utf-8"))
                command = payload.get("command")

                if command == "STOP" and (
                    self.current_fsm.state == States.RUNNING
                    or self.current_fsm.state == States.STARTING
                ):
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_STOP", retain=True, qos=2
                    )

                    # Stop running experiment
                    self.current_fsm.current_experiment.stop_thread()

                elif command == "START" and self.current_fsm.state == States.IDLE:
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_START", retain=True, qos=2
                    )

                    configs = payload.get("configs")
                    self.__log.info(f"[CLIENT] Received config: {configs}")

                    # Deserialize payload
                    configs = [
                        Config(self.__log, json.loads(config))
                        for config in json.loads(configs)
                    ]

                    # Set config in FSM
                    self.current_fsm.set_configs(configs)

                    # Trigger start transition
                    self.current_fsm_thread.trigger_start()

                elif command == "CLEAN":
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

                    # Send ack message
                    self.client.publish(
                        "experiment/ack", "ACK_CLEAN", retain=True, qos=2
                    )

                    # Trigger clean transition
                    self.current_fsm.clean_state()

                else:
                    self.__log.warning(
                        f"Received invalid command {command} for state {self.current_fsm.state}."
                    )
                    # Clean retained messages
                    self.client.publish("experiment/command", "", retain=True, qos=2)

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

    def update_state(self, state=None):

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
    log = Logger()
    log.info("[MONITOR] Starting experiment manager")
    fsm = ExperimentFSM(log)
    fsm_thread_wrapper = FSMThreadWrapper(fsm)
    fsm_thread_wrapper.start()

    client = MQTTClient(log, fsm_thread_wrapper)
    client.start()


if __name__ == "__main__":
    main()
