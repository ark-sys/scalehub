import paho.mqtt.client as mqtt
from .utils.Config import Config
from .utils.Logger import Logger

# 1. Create MQTT client
# 2. Connect to MQTT broker
# 3. Publish "start" or "stop" message on experiment topic


class Experiment:
    BROKER_HOST = "mqtt-broker.svc.cluster.local"

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.client = mqtt.Client()
        self.config = config

    def setup(self):
        self.client.connect(self.BROKER_HOST, 1883, 60)
        self.client.loop_start()

    def start(self):
        self.__log.info("Starting experiment")
        self.client.publish("experiment/start", self.config.to_json())

        # TODO: implement a mechanism to check if the experiment started correctly

    def stop(self):
        self.__log.info("Stopping experiment")
        self.client.publish("experiment/stop", "STOP")

        # TODO: implement a mechanism to check if the experiment stopped correctly
