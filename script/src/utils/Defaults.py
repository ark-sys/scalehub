class ConfigKeys:
    DEBUG_LEVEL = "debug.level"

    TYPE = "platform.type"
    SITE = "platform.site"
    CLUSTER = "platform.cluster"
    NUM_CONTROL = "platform.controllers"
    NUM_WORKERS = "platform.workers"
    QUEUE_TYPE = "platform.queue"
    WALLTIME = "platform.walltime"

    NAME = "experiment.name"
    JOB = "experiment.job_file"
    TASK = "experiment.task_name"
    DB_URL = "experiment.db_url"

    LOAD_GENERATORS = "experiment.load_generators"

    TOPIC_SOURCES = "experiment.topic_sources"
    NUM_SENSORS = "experiment.num_sensors"
    INTERVAL_MS = "experiment.interval_ms"

    PLAYBOOKS_PATH = "scalehub.playbook"
    INVENTORY_PATH = "scalehub.inventory"
    EXPERIMENTS_DATA_PATH = "scalehub.experiments"


class DefaultValues:
    class System:
        CONF_PATH = "/app/conf/scalehub.conf"
        PLAYBOOKS_PATH = "/app/playbooks/project"
        EXPERIMENTS_DATA_PATH = "/app/experiments-data"
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
        name = "scalehub"
        job_file = "myjoin-all.jar"
        task_name = "TumblingEventTimeWindows"
        db_url = "localhost:8428"

        class LoadGenerator:
            name = "load_generator"
            topic = "input-topic1"
            num_sensors = 100000
            interval_ms = 3000
            replicas = 1
