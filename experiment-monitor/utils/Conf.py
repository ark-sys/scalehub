import json

from .Logger import Logger


class Conf:
    __conf = {}

    def __init__(self, log: Logger, conf=None, log_path=None):
        self.__log = log

        # Save received config as dictionary
        if conf is not None:
            self.__conf = conf
        elif log_path is not None:
            self.load_from_log(log_path)

    def __str__(self):
        return self.to_str()

    def get(self, key) -> any:
        if key in self.__conf:
            return self.__conf[key]

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
        return json.dumps(self.__conf, indent=4)

    def load_from_log(self, log_path: str):
        with open(log_path, 'r') as f:
            lines = f.readlines()

        # Find the line number where [CONFIG] starts
        start_line = lines.index('[CONFIG]\n') + 1

        # Find the line number where [TIMESTAMPS] starts
        end_line = lines.index('[TIMESTAMPS]\n')

        # Join the lines between [CONFIG] and [TIMESTAMPS] and load as JSON
        config_content = ''.join(lines[start_line:end_line])
        self.__conf = json.loads(config_content)
