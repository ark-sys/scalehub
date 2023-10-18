class ConfigKeys:
    DEBUG_LEVEL = "debug.level"

    TYPE = "platform.type"
    SITE = "platform.site"
    CLUSTER = "platform.cluster"
    NUM_CONTROL = "platform.controllers"
    NUM_WORKERS = "platform.workers"
    QUEUE_TYPE = "platform.queue"
    WALLTIME = "platform.walltime"
    KUBERNETES_TYPE = "platform.kubernetes_type"

    NAME = "experiment.name"
    JOB = "experiment.job_file"
    TASK = "experiment.task_name"
    DB_URL = "experiment.db_url"

    LOAD_GENERATORS = "experiment.load_generators"
    DATA_SKIP_DURATION = "experiment.output.skip_s"
    DATA_OUTPUT_PLOT = "experiment.output.plot"
    DATA_OUTPUT_STATS = "experiment.output.stats"

    PLAYBOOKS_PATH = "scalehub.playbook"
    INVENTORY_PATH = "scalehub.inventory"
    EXPERIMENTS_DATA_PATH = "scalehub.experiments"

    TRANSCCALE_PAR = "transscale.max_parallelism"
    TRANSSCALE_WARMUP = "transscale.monitoring.warmup"
    TRANSSCALE_INTERVAL = "transscale.monitoring.interval"


class DefaultValues:
    class System:
        CONF_PATH = "/app/conf/scalehub.conf"
        PLAYBOOKS_PATH = "/app/playbooks/project"
        EXPERIMENTS_DATA_PATH = "/app/experiments-data"
        INVENTORY_PATH = "/tmp/hosts"

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
        kubernetes_type = "k3s"

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
            value = 5

        class ExperimentData:
            skip_s = 100
            stats = True
            plot = True

    class Transscale:
        max_parallelism = 10
        monitoring_warmup = 66
        monitoring_interval = 60
