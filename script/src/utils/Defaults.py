class DefaultKeys:
    class Scalehub:
        inventory = "scalehub.inventory"
        playbook = "scalehub.playbook"
        experiments = "scalehub.experiments"
        debug_level = "scalehub.debug_level"

    class Platform:
        type = "platform.type"
        reservation_name = "platform.reservation_name"
        site = "platform.site"
        cluster = "platform.cluster"
        producers = "platform.producers"
        consumers = "platform.consumers"
        queue = "platform.queue"
        walltime = "platform.walltime"
        start_time = "platform.start_time"
        kubernetes_type = "platform.kubernetes_type"

    class Experiment:
        name = "experiment.name"
        job_file = "experiment.job_file"
        task_name = "experiment.task_name"
        output_skip_s = "experiment.output_skip_s"
        output_stats = "experiment.output_stats"
        output_plot = "experiment.output_plot"
        broker_mqtt_host = "experiment.broker_mqtt_host"
        broker_mqtt_port = "experiment.broker_mqtt_port"

        class Generators:
            generators = "experiment.generators"

            class Generator:
                name = "experiment.generators.name"
                topic = "experiment.generators.name.topic"
                num_sensors = "experiment.generators.name.num_sensors"
                interval_ms = "experiment.generators.name.interval_ms"
                replicas = "experiment.generators.name.replicas"
                value = "experiment.generators.name.value"

        class Flink:
            checkpoint_interval_ms = "experiment.flink.checkpoint_interval_ms"
            window_size_ms = "experiment.flink.window_size_ms"
            fibonacci_value = "experiment.flink.fibonacci_value"

        class Transscale:
            max_parallelism = "experiment.transscale.max_parallelism"
            monitoring_warmup_s = "experiment.transscale.monitoring_warmup_s"
            monitoring_interval_s = "experiment.transscale.monitoring_interval_s"

        class Chaos:
            enable = "experiment.chaos.enable"
            delay_latency_ms = "experiment.chaos.delay_latency_ms"
            delay_jitter_ms = "experiment.chaos.delay_jitter_ms"
            delay_correlation = "experiment.chaos.delay_correlation"
            bandwidth_rate_mbps = "experiment.chaos.bandwidth_rate_mbps"
            bandwidth_limit = "experiment.chaos.bandwidth_limit"
            bandwidth_buffer = "experiment.chaos.bandwidth_buffer"


class DefaultValues:
    conf_path = "/app/conf/scalehub.ini"

    class Scalehub:
        # Path to where the playbooks are located
        playbooks = "/app/playbooks/project"
        # Base path to where experiments will be stored
        experiments = "/app/experiments-data"
        # Path to where the inventory  ansible inventory will be
        inventory = "/tmp/hosts"

        class Debug:
            DISABLED = 0
            LEVEL_1 = 1
            level = DISABLED

    class Platform:
        type = "Grid5000"
        reservation_name = "scalehub"
        site = "rennes"
        cluster = "parasilo"
        producers = 1
        consumers = 1
        queue = "default"
        walltime = "1:00:00"
        start_time = None
        kubernetes_type = "k3s"

    class Experiment:
        name = "scalehub"
        job_file = "myjoin-all.jar"
        task_name = "TumblingEventTimeWindows"
        output_skip_s = 120
        output_stats = True
        output_plot = True
        broker_mqtt_host = "broker-mqtt.scalehub.local"
        broker_mqtt_port = 1883

        class Generators:
            generators = ["generator1"]

            class Generator:
                name = "generator1"
                topic = "input-topic1"
                num_sensors = 100000
                interval_ms = 3000
                replicas = 1
                value = 5

        class Transscale:
            max_parallelism = 10
            monitoring_warmup_s = 66
            monitoring_interval_s = 60

        class Flink:
            checkpoint_interval_ms = 4000
            window_size_ms = 1000
            fibonacci_value = 20

        class Chaos:
            enable = False
            latency_ms = "25"
            jitter_ms = "0"
            correlation = "0"
            bandwidth_rate_mbps = 100
            bandwidth_limit = 1000
            bandwidth_buffer = 100
