from time import sleep

from scripts.src.resources.FlinkManager import FlinkManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class Scaling:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.config = config
        self.k = KubernetesManager(log)
        self.f = FlinkManager(log, config)
        # Load strategy from configuration
        self.steps = self.config.get(Key.Experiment.Scaling.steps)
        self.interval_scaling_s = self.config.get_int(
            Key.Experiment.Scaling.interval_scaling_s
        )
        self.max_parallelism = self.config.get_int(
            Key.Experiment.Scaling.max_parallelism
        )

        # Stop event
        self.stopped = None

    # Set callback for stop event
    def set_stopped_callback(self, stopped):
        self.stopped = stopped

    def scale_and_wait(self, tm_name, replicas):
        self.__log.info(
            f"[SCALING] ************ Scaling up {tm_name} to {replicas} ************"
        )
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name, replicas=replicas, namespace="flink"
        )
        self.f.rescale_job(replicas)
        self.__log.info(f"[SCALING] Waiting for {self.interval_scaling_s} seconds")
        # sleep(self.interval_scaling_s)
        # sleep unless stopped
        for i in range(self.interval_scaling_s):
            if self.stopped is not None and self.stopped():
                self.__log.info("[SCALING] Scaling stopped.")
                return 1
            sleep(1)

    # Add replicas linearly
    def scale_operator_linear(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.k.statefulset_manager.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )
        for i in range(number):
            # Scale up stateful set
            taskmanagers_count_dict[tm_type] += 1
            ret = self.scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
            if ret == 1:
                return 1

    # Add replicas exponentially
    def scale_operator_exponential(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.k.statefulset_manager.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )

        def get_scaling_sequence(number):
            pascaline_sequence = [1]
            i = 1
            while sum(pascaline_sequence) < number:
                pascaline_sequence.append(2**i)
                i += 1
            if sum(pascaline_sequence) > number:
                pascaline_sequence[-1] = number - sum(pascaline_sequence[:-1])
            return pascaline_sequence

        scaline_sequence = get_scaling_sequence(number)
        self.__log.info(f"[SCALING] Scaling sequence: {scaline_sequence}")

        for i in scaline_sequence:
            # Scale up stateful set
            taskmanagers_count_dict[tm_type] += i
            ret = self.scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
            if ret == 1:
                return 1

    # Add _number_ replicas at once
    def scale_operator_block(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.k.statefulset_manager.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )
        # Scale up stateful set
        taskmanagers_count_dict[tm_type] += number
        ret = self.scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
        if ret == 1:
            return 1

    def scale_op(self, taskmanager):
        number = taskmanager["number"]
        tm_type = taskmanager["type"]
        scaling_method = taskmanager["method"]

        match scaling_method:
            case "linear":
                ret = self.scale_operator_linear(number, tm_type)
            case "exponential":
                ret = self.scale_operator_exponential(number, tm_type)
            case "block":
                ret = self.scale_operator_block(number, tm_type)
            case _:
                self.__log.warning(
                    f"[SCALING] Scaling method {scaling_method} not supported. Defaulting to linear."
                )
                ret = self.scale_operator_linear(number, tm_type)
        if ret == 1:
            return 1

    def scale_step(self, step):
        self.__log.info(
            f"========================================= Step {step} ========================================="
        )
        node = self.steps[step]["node"]
        self.__log.info(f"[SCALING] Scaling on node : {node}")

        taskmanagers = self.steps[step]["taskmanager"]
        for taskmanager in taskmanagers:
            ret = self.scale_op(taskmanager)
            if ret == 1:
                return 1

    def run(self):
        node_name = self.setup_run()

        self.__log.info("[SCALING] First taskmanager, just waiting")
        sleep(self.interval_scaling_s)
        self.__log.info("[SCALING] Scaling started.")
        for i in range(len(self.steps)):
            try:
                if i > 0:
                    # Find a new node
                    node_type = self.steps[i]["node"]
                    if node_type == "vm_grid5000":
                        vm_type = self.steps[i]["type"]
                        next_node = self.k.node_manager.get_next_node(
                            node_type, vm_type
                        )
                    else:
                        next_node = self.k.node_manager.get_next_node(node_type)
                    if next_node:
                        self.__log.info(f"[SCALING] Next node: {next_node}")
                        # Mark this node with schedulable
                        node_name = next_node
                        self.k.node_manager.mark_node_as_schedulable(node_name)
                    else:
                        self.__log.error("[SCALING] No more nodes available.")
                        break
            except Exception as e:
                self.__log.error(f"[SCALING] Error while getting next node: {e}")
                break
            current_state_taskmanagers = (
                self.k.statefulset_manager.get_count_of_taskmanagers()
            )
            self.__log.info(
                f"[SCALING] Current statefulset taskmanagers: {current_state_taskmanagers}"
            )
            # Expected state at the end of step
            expected_state_taskmanagers = self.steps[i]["taskmanager"]
            self.__log.info(
                f"[SCALING] Expected statefulset taskmanagers: {expected_state_taskmanagers}"
            )

            # Scale step
            ret = self.scale_step(i)
            if ret == 1:
                self.__log.info("[SCALING] Scaling is finishing due to stop event.")
                return 1
            self.__log.info(
                f"[SCALING] Scaling step on node {node_name} finished. Marking node as full."
            )
            self.k.node_manager.mark_node_as_full(node_name)

            current_state_taskmanagers = (
                self.k.statefulset_manager.get_count_of_taskmanagers()
            )
            self.__log.info(
                f"[SCALING] Current statefulset taskmanagers: {current_state_taskmanagers}"
            )
            sleep(5)
        self.__log.info("[SCALING] Scaling finished.")

    def setup_run(self):
        self.__log.info("[SCALING] Setting up experiment.")
        # Get the first node to scale based on what's defined in the strategy file
        node_type = self.steps[0]["node"]
        if node_type == "vm_grid5000":
            vm_type = self.steps[0]["type"]
            first_node = self.k.node_manager.get_next_node(node_type, vm_type)
        else:
            first_node = self.k.node_manager.get_next_node(node_type)

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

        # Decrement the number of taskmanagers from strategy
        # TODO: find a better way to do this
        self.steps[0]["taskmanager"][0]["number"] -= 1
        if self.steps[0]["taskmanager"][0]["number"] == 0:
            self.steps[0]["taskmanager"].pop(0)
            self.__log.info(
                "[SCALING] First taskmanager scaled, removing from strategy."
            )
        if len(self.steps[0]["taskmanager"]) == 0:
            self.steps.pop(0)
            self.__log.info("[SCALING] First node scaled, removing from strategy.")
        if len(self.steps) == 0:
            self.__log.info("[SCALING] No more steps to scale.")

        return first_node
