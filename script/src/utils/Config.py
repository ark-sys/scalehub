from os import getcwd
from os.path import exists, join

from .Defaults import DefaultValues as Value, ConfigKeys as Key
from .Logger import Logger


class Config:
    __config = {}

    def __init__(self, log: Logger, conf_path: str = None):
        self.__log = log
        self.__init_defaults()

        if conf_path is None or not exists(conf_path):
            self.__log.warning(
                f"[CONFIG] Config file [{conf_path}] not specified or not existing.\n"
                f"\tUsing [{Value.System.CONF_PATH}] instead."
            )
            conf_path = join(getcwd(), Value.System.CONF_PATH)
        self.__read_config_file(conf_path)

    def __init_defaults(self):
        self.__config[Key.PLAYBOOKS_PATH] = Value.System.PLAYBOOKS_PATH
        self.__config[Key.INVENTORY_PATH] = Value.System.INVENTORY_PATH
        self.__config[Key.EXPERIMENTS_DATA_PATH] = Value.System.EXPERIMENTS_DATA_PATH
        self.__config[Key.DEBUG_LEVEL] = Value.System.Debug.level

        self.__config[Key.TYPE] = Value.Platform.type
        self.__config[Key.SITE] = Value.Platform.site
        self.__config[Key.CLUSTER] = Value.Platform.cluster
        self.__config[Key.NUM_CONTROL] = Value.Platform.controllers
        self.__config[Key.NUM_WORKERS] = Value.Platform.workers
        self.__config[Key.QUEUE_TYPE] = Value.Platform.queue
        self.__config[Key.WALLTIME] = Value.Platform.walltime

        self.__config[Key.NAME] = Value.Experiment.name
        self.__config[Key.JOB] = Value.Experiment.job_file
        self.__config[Key.TASK] = Value.Experiment.task_name
        self.__config[Key.DB_URL] = Value.Experiment.db_url

        self.__config[Key.LOAD_GENERATORS] = Value.Experiment.LoadGenerator
        self.__config[Key.DATA_SKIP_DURATION] = Value.Experiment.ExperimentData.skip_s
        self.__config[Key.DATA_OUTPUT_PLOT] = Value.Experiment.ExperimentData.plot
        self.__config[Key.DATA_OUTPUT_STATS] = Value.Experiment.ExperimentData.stats

        self.__config[Key.TRANSCCALE_PAR] = Value.Transscale.max_parallelism
        self.__config[Key.TRANSSCALE_WARMUP] = Value.Transscale.monitoring_warmup
        self.__config[Key.TRANSSCALE_INTERVAL] = Value.Transscale.monitoring_interval


    def get(self, key) -> any:
        if key in self.__config:
            return self.__config[key]

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_bool(self, key) -> bool:
        return bool(self.get(key))

    def get_float(self, key) -> float:
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def get_list_str(self, key):
        return [str(value) for value in self.get_str(key).split(",")]

    def get_list_int(self, key):
        return [int(value) for value in self.get_str(key).split(",")]

    def parse_load_generators(self):
        load_generators_section = self.get("experiment.load_generators")

        load_generators = []
        current_generator = None

        for line in load_generators_section.split("\n"):
            line = line.strip()
            if not line:
                continue

            if line.startswith("- name"):
                if current_generator:
                    load_generators.append(current_generator)
                current_generator = {"name": line.split("=")[1].strip()}
            else:
                key, value = map(str.strip, line.split("="))
                current_generator[key] = value

        if current_generator:
            load_generators.append(current_generator)

        return load_generators

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
                    self.__log.error(f'[CONF] Specified key "{key}" does not exist')
        else:
            self.__log.warning(f"[CONF] No configuration file found")

    def validate(self):
        # TODO validate config file format (stuff like minimum info)
        pass
