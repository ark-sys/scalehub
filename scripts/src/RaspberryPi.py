from scripts.src.Platform import Platform
from scripts.utils.Logger import Logger
from scripts.utils.Config import Config


# TODO: Implement RaspberryPi class
class RaspberryPi(Platform):
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config

    def setup(self) -> str:
        pass
