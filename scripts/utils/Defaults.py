metrics_dict = {
    "operator_metrics": [
        "flink_taskmanager_job_task_numRecordsInPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ],
    "sources_metrics": ["flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"],
    "state_metrics": [
        "flink_jobmanager_job_lastCheckpointSize",
        "flink_jobmanager_job_lastCheckpointDuration",
    ],
    "job_metrics": [
        "flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength",
        "flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength",
        "flink_taskmanager_job_task_estimatedTimeToConsumeBuffersMs",
    ],
}

MAP_PIPELINE_DICT = {1: "Source:_Source", 2: "Map", 3: "Sink:_Sink"}
JOIN_PIPELINE_DICT = {
    1: ("Source:_Source1", "Source:_Source2"),
    2: "Timestamps_Watermarks____Map",
    3: "TumblingEventTimeWindows____Timestamps_Watermarks",
    4: "Sink:_Sink",
}


class DefaultKeys:
    class Scalehub:
        inventory = "scalehub.inventory"
        playbook = "scalehub.playbook"
        experiments = "scalehub.experiments"
        debug_level = "scalehub.debug_level"

    class Platforms:
        platforms = "platforms"

        class Platform:
            name = "platforms.name"
            type = "platforms.name.type"
            reservation_name = "platforms.name.reservation_name"
            site = "platforms.name.site"
            cluster = "platforms.name.cluster"
            producers = "platforms.name.producers"
            consumers = "platforms.name.consumers"
            core_per_vm = "platforms.name.core_per_vm"
            memory_per_vm = "platforms.name.memory_per_vm"
            disk_per_vm = "platforms.name.disk_per_vm"
            queue = "platforms.name.queue"
            walltime = "platforms.name.walltime"
            start_time = "platforms.name.start_time"
            kubernetes_type = "platforms.name.kubernetes_type"
            archi = "platforms.name.archi"
            control = "platforms.name.control"

    class Experiment:
        name = "experiment.name"
        job_file = "experiment.job_file"
        task_name = "experiment.task_name"
        output_skip_s = "experiment.output_skip_s"
        output_stats = "experiment.output_stats"
        output_plot = "experiment.output_plot"
        broker_mqtt_host = "experiment.broker_mqtt_host"
        broker_mqtt_port = "experiment.broker_mqtt_port"
        kafka_partitions = "experiment.kafka_partitions"
        first_node = "experiment.first_node"
        unchained_tasks = "experiment.unchained_tasks"

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
            affected_nodes_percentage = "experiment.chaos.affected_nodes_percentage"
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

    class Platforms:
        platforms = ["grid5000"]

        class Platform:
            type = "Grid5000"
            reservation_name = "scalehub"
            site = "rennes"
            cluster = "parasilo"
            producers = 1
            consumers = 1
            queue = "default"
            walltime = "1:00:00"
            core_per_vm = 2
            memory_per_vm = 4096
            disk_per_vm = 40
            start_time = None
            kubernetes_type = "k3s"
            archi = "rpi3:at86rf233"
            # This platform reservation contains a control node
            control = True

    class Experiment:
        name = "scalehub"
        job_file = "myjoin-all.jar"
        task_name = "TumblingEventTimeWindows"
        output_skip_s = 120
        output_stats = True
        output_plot = True
        broker_mqtt_host = "broker-mqtt.scalehub.local"
        broker_mqtt_port = 1883
        kafka_partitions = 1000
        first_node = "grid5000"
        unchained_tasks = False

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
            fibonacci_value = 18

        class Chaos:
            enable = False
            affected_nodes_percentage = 50
            latency_ms = 25
            jitter_ms = 0
            correlation = 0
            bandwidth_rate_mbps = 100
            bandwidth_limit = 1000
            bandwidth_buffer = 100
