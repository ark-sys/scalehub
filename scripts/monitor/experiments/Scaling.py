from time import sleep

from scripts.src.resources.FlinkManager import FlinkManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class Scaling:
    def __init__(self, log: Logger, config: Config, km: KubernetesManager):
        self.__log = log
        self.k = km
        self.f = FlinkManager(log, config, self.k)
        # Load strategy from configuration
        self.steps = config.get(Key.Experiment.Scaling.steps)
        self.interval_scaling_s = config.get_int(
            Key.Experiment.Scaling.interval_scaling_s
        )

        # Stop event
        self.__stopped = None

    # Set callback for stop event
    def set_stopped_callback(self, stopped):
        self.__stopped = stopped

    def __wait_interval(self):
        wait_time = self.interval_scaling_s
        self.__log.info(f"[SCALING] Monitoring interval: for {wait_time} seconds")
        # sleep unless stopped
        for i in range(wait_time):
            if self.__stopped is not None and self.__stopped():
                self.__log.info("[SCALING] Scaling stopped.")
                return 1
            sleep(1)
        return 0

    def __scale_and_wait(self, replicas):
        self.__log.info(f"[SCALING] ************ Scaling up to {replicas} ************")
        ret = self.f.run_job(replicas)
        if ret == 1:
            return 1
        else:
            # Rescale successful, populate job info
            self.f.get_job_info()
            self.f.check_nominal_job_run()
            ret = self.f.wait_for_job_running()
            if ret == 1:
                return 1
            return self.__wait_interval()

    def __scale_w_tm(self, replicas, tm_type):
        self.__log.info(f"[SCALING] ************ Scaling up to {replicas} ************")
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.k.statefulset_manager.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )
        # Scale up stateful set
        new_tm_count = taskmanagers_count_dict[tm_type] + replicas
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name,
            replicas=new_tm_count,
            namespace="flink",
        )
        # Wait for the stateful set to be ready
        while True:
            self.__log.info("[SCALING] Waiting for taskmanagers to be ready.")
            if (
                self.k.statefulset_manager.get_count_of_taskmanagers()[tm_type]
                == new_tm_count
            ):
                break
            sleep(2)

        ret = self.__scale_and_wait(taskmanagers_count_dict[tm_type])
        if ret == 1:
            return 1

    # Add replicas linearly
    def __scale_linear(self, number, tm_type, scope):
        match scope:
            case "taskmanager":
                for i in range(number):
                    # Scale up stateful set
                    ret = self.__scale_w_tm(1, tm_type)
                    if ret == 1:
                        return 1
            case "slots":
                # Get current parallelism of monitored task
                current_parallelism = self.f.monitored_task_parallelism
                self.__log.info(
                    f"[SCALING] Current parallelism of monitored task: {current_parallelism}"
                )
                for i in range(number):
                    # Scale up operator
                    current_parallelism += 1
                    ret = self.__scale_and_wait(current_parallelism)
                    if ret == 1:
                        return 1
            case _:
                self.__log.warning(
                    f"[SCALING] Scope {scope} not supported. Defaulting to taskmanager."
                )
                for i in range(number):
                    # Scale up stateful set
                    ret = self.__scale_w_tm(1, tm_type)
                    if ret == 1:
                        return 1

    # Add replicas exponentially
    def __scale_exponential(self, number, tm_type, scope):
        def __get_scaling_sequence(seq_n):
            pascaline_sequence = [1]
            i = 1
            while sum(pascaline_sequence) < seq_n:
                pascaline_sequence.append(2**i)
                i += 1
            if sum(pascaline_sequence) > seq_n:
                pascaline_sequence[-1] = seq_n - sum(pascaline_sequence[:-1])
            return pascaline_sequence

        scaline_sequence = __get_scaling_sequence(number)

        match scope:
            case "taskmanager":
                for i in scaline_sequence:
                    # Scale up stateful set
                    ret = self.__scale_w_tm(i, tm_type)
                    if ret == 1:
                        return 1
            case "slots":
                # Get current parallelism of monitored task
                current_parallelism = self.f.monitored_task_parallelism
                self.__log.info(
                    f"[SCALING] Current parallelism of monitored task: {current_parallelism}"
                )
                for i in scaline_sequence:
                    # Scale up operator
                    current_parallelism += i
                    ret = self.__scale_and_wait(current_parallelism)
                    if ret == 1:
                        return 1
            case _:
                self.__log.warning(
                    f"[SCALING] Scope {scope} not supported. Defaulting to taskmanager."
                )
                for i in scaline_sequence:
                    # Scale up stateful set
                    ret = self.__scale_w_tm(i, tm_type)
                    if ret == 1:
                        return 1

    # Add replicas at once
    def __scale_block(self, number, tm_type, scope):
        match scope:
            case "taskmanager":
                # Get current number of taskmanagers
                return self.__scale_w_tm(number, tm_type)
            case "slots":
                # Get current parallelism of monitored task
                current_parallelism = self.f.monitored_task_parallelism
                self.__log.info(
                    f"[SCALING] Current parallelism of monitored task: {current_parallelism}"
                )
                # Scale up operator
                new_parallelism = current_parallelism + number
                return self.__scale_and_wait(new_parallelism)
            case _:
                self.__log.warning(
                    f"[SCALING] Scope {scope} not supported. Defaulting to taskmanager."
                )
                return self.__scale_w_tm(number, tm_type)

    def __scale(self, taskmanager):
        number = taskmanager["number"]
        tm_type = taskmanager["type"]
        scaling_method = taskmanager["method"]
        scope = taskmanager["scope"] if "scope" in taskmanager else "taskmanager"

        match scaling_method:
            case "linear":
                ret = self.__scale_linear(number, tm_type, scope)
            case "exponential":
                ret = self.__scale_exponential(number, tm_type, scope)
            case "block":
                ret = self.__scale_block(number, tm_type, scope)
            case _:
                self.__log.warning(
                    f"[SCALING] Scaling method {scaling_method} not supported. Defaulting to linear."
                )
                ret = self.__scale_linear(number, tm_type)
        if ret == 1:
            return 1

    def __scale_step(self, step):
        self.__log.info(
            f"========================================= Step {step} ========================================="
        )
        node = self.steps[step]["node"]
        self.__log.info(f"[SCALING] Scaling on node : {node}")

        taskmanagers = self.steps[step]["taskmanager"]
        for taskmanager in taskmanagers:
            ret = self.__scale(taskmanager)
            if ret == 1:
                return 1

    def __get_scaling_node(self, step, node_name):
        if step > 0:
            node_type = self.steps[step]["node"]
            vm_type = self.steps[step]["type"] if node_type == "vm_grid5000" else None
            next_node = self.k.node_manager.get_next_node(node_type, vm_type)
            if next_node:
                self.__log.info(f"[SCALING] Next node: {next_node}")
                self.k.node_manager.mark_node_as_schedulable(next_node)
                return next_node, "pass"
            else:
                self.__log.error("[SCALING] No more nodes available.")
                return None, "break"
        elif step == 0:
            if len(self.steps[step]["taskmanager"]) == 1:
                if self.steps[step]["taskmanager"][0]["number"] == 1:
                    self.__log.info(
                        "[SCALING] First node and first taskmanager already scaled."
                    )
                    self.k.node_manager.mark_node_as_full(node_name)
                    return node_name, "continue"
                else:
                    self.steps[step]["taskmanager"][0]["number"] -= 1
            else:
                if self.steps[step]["taskmanager"][0]["number"] == 1:
                    self.__log.info(
                        "[SCALING] First node and first taskmanager already scaled."
                    )
                    self.steps[step]["taskmanager"].pop(0)
            return node_name, "pass"
        else:
            self.__log.error(
                "[SCALING] What is happening? i can't be less than 0. Continuing."
            )
            return node_name, "break"

    def __setup_run(self):
        self.__log.info("[SCALING] Setting up experiment.")
        # Reset scaling labels, clean start.
        self.k.node_manager.reset_scaling_labels()
        # Reset state labels
        self.k.node_manager.reset_state_labels()

        # Get the first node to scale based on what's defined in the strategy file
        node_type = self.steps[0]["node"]
        if node_type == "vm_grid5000":
            vm_type = self.steps[0]["type"]
            first_node = self.k.node_manager.get_next_node(node_type, vm_type)
        else:
            first_node = self.k.node_manager.get_next_node(node_type)
        if not first_node:
            self.__log.error("[SCALING] No node available.")
            return 1

        self.__log.info(f"[SCALING] First node: {first_node}")

        # Mark this node with schedulable
        self.k.node_manager.mark_node_as_schedulable(first_node)

        # Get first taskmanager to deploy
        taskmanager_type = self.steps[0]["taskmanager"][0]["type"]

        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{taskmanager_type}"

        self.__log.debug(f"[SCALING] Scaling up {tm_name} to 1")

        # Scale up stateful set
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name, replicas=1, namespace="flink"
        )

        # Start the job
        ret = self.f.run_job()
        if ret == 1:
            return 1

        # Populate job info
        self.f.get_job_info()
        self.f.check_nominal_job_run()

        return first_node

    def run(self):
        node_name = self.__setup_run()
        if node_name == 1:
            return 1

        self.__log.info("[SCALING] First taskmanager, just waiting...")
        ret = self.__wait_interval()
        if ret == 1:
            return 1
        else:
            self.__log.info("[SCALING] Scaling started.")
            # Iterate over each step of the strategy (each step is a node)
            for step in range(len(self.steps)):
                node_name, action = self.__get_scaling_node(step, node_name)
                match action:
                    case "break":
                        break
                    case "continue":
                        continue
                    case _:
                        pass
                # Scale step
                ret = self.__scale_step(step)
                if ret == 1:
                    self.__log.info("[SCALING] Scaling is finishing due to stop event.")
                    return 1
                self.__log.info(
                    f"[SCALING] Scaling step on node {node_name} finished. Marking node as full."
                )
                self.k.node_manager.mark_node_as_full(node_name)
                sleep(5)
        self.__log.info("[SCALING] Scaling finished.")
