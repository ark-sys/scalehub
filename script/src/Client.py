import os
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
        self.setup_mqtt()

        self.acked = False

    def on_message(self, client, userdata, message):

        match message.topic:
            case "ack/experiment/start":
                if message.payload == "RUNNING":
                    self.acked = True
            case "ack/experiment/stop":
                if message.payload == "STOPPING":
                    self.acked = True

    def on_connect(self, client, userdata, flags, rc):
        self.__log.info(f"Connected with result code {rc}")

        # Subscribe to experiment topics
        self.client.subscribe("ack/experiment/#", qos=2)

    def setup_mqtt(self):
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()

    def start(self):
        self.__log.info("Starting experiment")

        # Check if chaos is enabled
        if self.config.get_bool(Key.Experiment.Chaos.enable):
            chaos_params = {
                "delay_latency_ms": self.config.get_int(
                    Key.Experiment.Chaos.delay_latency_ms
                ),
                "delay_jitter_ms": self.config.get_int(
                    Key.Experiment.Chaos.delay_jitter_ms
                ),
                "delay_correlation": self.config.get_float(
                    Key.Experiment.Chaos.delay_correlation
                ),
            }
            self.p.run_playbook(
                "chaos", config=self.config, tag="experiment", extra_vars=chaos_params
            )

        # Deploy load generators
        for generator in self.config.get(Key.Experiment.Generators.generators):
            load_generator_params = {
                "lg_name": generator["name"],
                "lg_topic": generator["topic"],
                "lg_numsensors": int(generator["num_sensors"]),
                "lg_intervalms": int(generator["interval_ms"]),
                "lg_replicas": int(generator["replicas"]),
                "lg_value": int(generator["value"]),
            }
            self.p.run_playbook(
                "load_generators",
                config=self.config,
                tag="create",
                extra_vars=load_generator_params,
            )

        # Send message to remote experiment-monitor to start experiment
        self.client.publish(
            "experiment/start",
            payload=self.config.to_json(),
            qos=2,
            retain=True,
        )

        # Wait message on ack/experiment/start
        while not self.acked:
            self.__log.info("Waiting for ack...")
            time.sleep(1)  # wait for 1 second before checking again
        self.__log.info("Experiment is running.")

    def stop(self):
        self.__log.info("Stopping experiment")
        # Send stop message to remote experiment-monitor
        self.client.publish("experiment/stop", "STOP", qos=2, retain=True)

        # Wait message on ack/experiment/stop
        while not self.acked:
            self.__log.info("Waiting for ack...")
            time.sleep(1)
        self.__log.info("Experiment stopped.")
