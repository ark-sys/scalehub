from os.path import join


class ConfigKeys:

    TYPE = "platform.type"
    SITE = "platform.site"
    CLUSTER = "platform.cluster"
    NUM_CONTROL = "platform.controllers"
    NUM_WORKERS = "platform.workers"
    QUEUE_TYPE = "platform.queue"
    WALLTIME = "platform.walltime"

    NAME = "experiment.name"
    TOPIC_SOURCES = "experiment.sources"
    NUM_SENSORS = "experiment.sensors"
    INTERVAL_MS = "experiment.interval"

    DEBUG_LEVEL = "debug.level"


class DefaultValues:
    class System:
        CONF_PATH = "/app/conf/scalehub.conf"
        PLAYBOOKS_PATH = "/app/playbooks"
        INVENTORY_PATH = "/tmp/hosts.json"
        class Debug:
            DISABLED = 0
            LEVEL_1 = 1

            level = DISABLED

    class Platform:
        type = "Grid5000"
        site = "rennes"
        cluster = "parasilo"
        controllers = 1
        workers = 2
        queue = "default"
        walltime = "1:00:00"

    class Experiment:
        name = "scalehub1"
        sources = ["input-topic1"]
        sensors = 100000
        interval = 3000