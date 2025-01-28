class ConfigKey:
    def __init__(self, key: str, is_optional: bool = False, *args, **kwargs):
        self.key = key
        self.is_optional = is_optional
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return self.key


# Defaults.py
from dataclasses import dataclass

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


@dataclass
class DefaultKeys:
    class Scalehub:
        inventory = ConfigKey("scalehub.inventory", is_optional=False)
        playbook = ConfigKey("scalehub.playbook", is_optional=False)
        experiments = ConfigKey("scalehub.experiments", is_optional=False)
        debug_level = ConfigKey("scalehub.debug_level", is_optional=False)

    class Platforms:
        platforms = ConfigKey("platforms", is_optional=True)

        class Platform:
            name = ConfigKey("platforms.name", is_optional=False)
            type = ConfigKey("platforms.name.type", is_optional=False)
            reservation_name = ConfigKey(
                "platforms.name.reservation_name",
                is_optional=False,
                for_types=["Grid5000", "FIT", "VM_on_Grid5000"],
            )
            site = ConfigKey(
                "platforms.name.site",
                is_optional=False,
                for_types=["Grid5000", "FIT", "VM_on_Grid5000"],
            )
            cluster = ConfigKey(
                "platforms.name.cluster",
                is_optional=False,
                for_types=["Grid5000", "FIT", "VM_on_Grid5000"],
            )
            producers = ConfigKey("platforms.name.producers", is_optional=False)
            consumers = ConfigKey("platforms.name.consumers", is_optional=False)
            core_per_vm = ConfigKey(
                "platforms.name.core_per_vm",
                is_optional=False,
                for_types=["VM_on_Grid5000"],
            )
            memory_per_vm = ConfigKey(
                "platforms.name.memory_per_vm",
                is_optional=False,
                for_types=["VM_on_Grid5000"],
            )
            disk_per_vm = ConfigKey(
                "platforms.name.disk_per_vm",
                is_optional=False,
                for_types=["VM_on_Grid5000"],
            )
            queue = ConfigKey(
                "platforms.name.queue",
                is_optional=False,
                for_types=["VM_on_Grid5000", "Grid5000", "FIT"],
            )
            walltime = ConfigKey(
                "platforms.name.walltime",
                is_optional=False,
                for_types=["VM_on_Grid5000", "Grid5000", "FIT"],
            )
            start_time = ConfigKey(
                "platforms.name.start_time",
                is_optional=False,
                for_types=["VM_on_Grid5000", "Grid5000", "FIT"],
            )
            kubernetes_type = ConfigKey(
                "platforms.name.kubernetes_type", is_optional=True
            )
            archi = ConfigKey(
                "platforms.name.archi", is_optional=False, for_types=["FIT"]
            )
            control = ConfigKey(
                "platforms.name.control",
                is_optional=False,
                for_types=["VM_on_Grid5000", "Grid5000"],
            )

    class Experiment:
        name = ConfigKey("experiment.name", is_optional=False)
        job_file = ConfigKey("experiment.job_file", is_optional=False)
        task_name = ConfigKey("experiment.task_name", is_optional=False)
        output_skip_s = ConfigKey("experiment.output_skip_s", is_optional=False)
        output_stats = ConfigKey("experiment.output_stats", is_optional=False)
        output_plot = ConfigKey("experiment.output_plot", is_optional=False)
        broker_mqtt_host = ConfigKey("experiment.broker_mqtt_host", is_optional=False)
        broker_mqtt_port = ConfigKey("experiment.broker_mqtt_port", is_optional=False)
        kafka_partitions = ConfigKey("experiment.kafka_partitions", is_optional=False)
        unchained_tasks = ConfigKey("experiment.unchained_tasks", is_optional=False)
        type = ConfigKey("experiment.type", is_optional=False)
        runs = ConfigKey("experiment.runs", is_optional=False)
        comment = ConfigKey("experiment.comment", is_optional=True)

        class Scaling:
            strategy_path = ConfigKey(
                "experiment.scaling.strategy_path", is_optional=True
            )
            interval_scaling_s = ConfigKey(
                "experiment.scaling.interval_scaling_s", is_optional=True
            )
            max_parallelism = ConfigKey(
                "experiment.scaling.max_parallelism", is_optional=True
            )
            steps = ConfigKey("experiment.scaling.steps", is_optional=True)

        class Generators:
            generators = ConfigKey("experiment.generators", is_optional=True)

            class Generator:
                name = ConfigKey("experiment.generators.name", is_optional=False)
                topic = ConfigKey("experiment.generators.name.topic", is_optional=False)
                type = ConfigKey("experiment.generators.name.type", is_optional=False)
                num_sensors = ConfigKey(
                    "experiment.generators.name.num_sensors", is_optional=False
                )
                interval_ms = ConfigKey(
                    "experiment.generators.name.interval_ms", is_optional=False
                )
                replicas = ConfigKey(
                    "experiment.generators.name.replicas", is_optional=False
                )
                value = ConfigKey("experiment.generators.name.value", is_optional=False)

        class Flink:
            checkpoint_interval_ms = ConfigKey(
                "experiment.flink.checkpoint_interval_ms", is_optional=False
            )
            window_size_ms = ConfigKey(
                "experiment.flink.window_size_ms", is_optional=False
            )
            fibonacci_value = ConfigKey(
                "experiment.flink.fibonacci_value", is_optional=False
            )

        class Transscale:
            max_parallelism = ConfigKey(
                "experiment.transscale.max_parallelism", is_optional=True
            )
            monitoring_warmup_s = ConfigKey(
                "experiment.transscale.monitoring_warmup_s", is_optional=True
            )
            monitoring_interval_s = ConfigKey(
                "experiment.transscale.monitoring_interval_s", is_optional=True
            )

        class Chaos:
            enable = ConfigKey("experiment.chaos.enable", is_optional=True)
            affected_nodes_percentage = ConfigKey(
                "experiment.chaos.affected_nodes_percentage", is_optional=True
            )
            delay_latency_ms = ConfigKey(
                "experiment.chaos.delay_latency_ms", is_optional=True
            )
            delay_jitter_ms = ConfigKey(
                "experiment.chaos.delay_jitter_ms", is_optional=True
            )
            delay_correlation = ConfigKey(
                "experiment.chaos.delay_correlation", is_optional=True
            )
            bandwidth_rate_mbps = ConfigKey(
                "experiment.chaos.bandwidth_rate_mbps", is_optional=True
            )
            bandwidth_limit = ConfigKey(
                "experiment.chaos.bandwidth_limit", is_optional=True
            )
            bandwidth_buffer = ConfigKey(
                "experiment.chaos.bandwidth_buffer", is_optional=True
            )
