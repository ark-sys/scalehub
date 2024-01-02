import json

from .Logger import Logger


class Config:
    __config = {}

    def __init__(self, log: Logger, config):
        self.__log = log

        # Save received config as dictionary
        self.__config = config

    def get(self, key) -> any:
        if key in self.__config:
            return self.__config[key]

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_bool(self, key) -> bool:
        if self.get_str(key).lower() == "true":
            return True
        else:
            return False

    def get_float(self, key) -> float:
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def get_list_str(self, key):
        return [str(value) for value in self.get_str(key).split(",")]

    def get_list_int(self, key):
        return [int(value) for value in self.get_str(key).split(",")]

    def to_str(self):
        # Pretty print config as string
        return json.dumps(self.__config, indent=4)
