from .Defaults import DefaultValues as Value, ConfigKeys as Key
from .Logger import Logger
from os.path import exists, join
from os import getcwd
class Config:
    __config = {}
    def __init__(self, log: Logger, conf_path: str = None):
        self.__log = log
        self.__init_defaults()

        if conf_path is None or not exists(conf_path):
            self.__log.warning(f"[CONFIG] Config file [{conf_path}] not specified or not existing.\n"
                               f"\tUsing [{Value.System.CONF_PATH}] instead.")
            conf_path = join(getcwd(), Value.System.CONF_PATH)
        self.__read_config_file(conf_path)
    def __init_defaults(self):
        self.__config[Key.TYPE] = Value.Platform.type
        self.__config[Key.SITE] = Value.Platform.site
        self.__config[Key.CLUSTER] = Value.Platform.cluster
        self.__config[Key.NUM_CONTROL] = Value.Platform.controllers
        self.__config[Key.NUM_WORKERS] = Value.Platform.workers
        self.__config[Key.QUEUE_TYPE] = Value.Platform.queue
        self.__config[Key.WALLTIME] = Value.Platform.walltime

        self.__config[Key.NAME] = Value.Experiment.name
        self.__config[Key.TOPIC_SOURCES] = Value.Experiment.topic_sources
        self.__config[Key.NUM_SENSORS] = Value.Experiment.num_sensors
        self.__config[Key.INTERVAL_MS] = Value.Experiment.interval_ms

        self.__config[Key.DEBUG_LEVEL] = Value.System.Debug.level

    def get(self, key) -> any:
        if key in self.__config:
            return self.__config[key]

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_float(self, key) -> float:
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def set(self, key, value):
        self.__config[key] = value

    def __read_config_file(self, conf_path: str):
        if exists(conf_path):

            import configparser as cp
            dummy_header = "config"

            parser = cp.ConfigParser()
            with open(conf_path) as cf:
                content = f"[{dummy_header}]\n" + cf.read()

            parser.read_string(content)
            conf = parser[dummy_header]

            for key in conf:
                if key in self.__config:
                    self.__config[key] = conf[key]
                else:
                    self.__log.error(f"[CONF] Specified key \"{key}\" does not exist")
        else:
            self.__log.warning(f"[CONF] No configuration file found")

    def validate(self):
        #TODO validate config file format (stuff like minimum info)
        pass