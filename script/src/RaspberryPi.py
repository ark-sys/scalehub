from .Platform import Platform
from .utils import Logger
from .utils.Config import Config, Key


# TODO: Implement RaspberryPi class
class RaspberryPi(Platform):

    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config

    def setup(self) -> str:
        pass
