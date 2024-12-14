from time import sleep

from scripts.src.resources.FlinkManager import FlinkManager
from scripts.src.resources.KubernetesManager import KubernetesManager
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class Scaling:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.k = KubernetesManager(log)
        self.f = FlinkManager(log, config)
        # Load strategy from configuration
        self.steps = config.get(Key.Experiment.Scaling.steps)
        self.interval_scaling_s = config.get_int(
            Key.Experiment.Scaling.interval_scaling_s
        )
        self.max_parallelism = config.get_int(Key.Experiment.Scaling.max_parallelism)

        # Stop event
        self.stopped = None

    # Set callback for stop event
    def set_stopped_callback(self, stopped):
        self.stopped = stopped

    def __wait_interval(self, extra_time=0):
        wait_time = self.interval_scaling_s + extra_time
        self.__log.info(f"[SCALING] Waiting for {wait_time} seconds")
        # sleep unless stopped
        for i in range(wait_time):
            if self.stopped is not None and self.stopped():
                self.__log.info("[SCALING] Scaling stopped.")
                return 1
            sleep(1)
        return 0

    def __scale_and_wait(self, tm_name, replicas):
        self.__log.info(
            f"[SCALING] ************ Scaling up {tm_name} to {replicas} ************"
        )
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name, replicas=replicas, namespace="flink"
        )
        ret = self.f.rescale_job(replicas)
        if ret == 1:
            return 1
        else:
            if tm_name == "flink-taskmanager-s":
                # Small may take some time to fully startup, especially for Flink
                return self.__wait_interval(extra_time=30)
            else:
                return self.__wait_interval()

    # Add replicas linearly
    def __scale_operator_linear(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.f.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )
        for i in range(number):
            # Scale up stateful set
            taskmanagers_count_dict[tm_type] += 1
            ret = self.__scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
            if ret == 1:
                return 1

    # Add replicas exponentially
    def __scale_operator_exponential(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.f.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )

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
        self.__log.info(f"[SCALING] Scaling sequence: {scaline_sequence}")

        for i in scaline_sequence:
            # Scale up stateful set
            taskmanagers_count_dict[tm_type] += i
            ret = self.__scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
            if ret == 1:
                return 1

    # Add _number_ replicas at once
    def __scale_operator_block(self, number, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"
        # Get current number of taskmanagers
        taskmanagers_count_dict = self.f.get_count_of_taskmanagers()
        self.__log.info(
            f"[SCALING] Current number of taskmanagers: {taskmanagers_count_dict}"
        )
        # Scale up stateful set
        taskmanagers_count_dict[tm_type] += number
        ret = self.__scale_and_wait(tm_name, taskmanagers_count_dict[tm_type])
        if ret == 1:
            return 1

    def __scale_op(self, taskmanager):
        number = taskmanager["number"]
        tm_type = taskmanager["type"]
        scaling_method = taskmanager["method"]

        match scaling_method:
            case "linear":
                ret = self.__scale_operator_linear(number, tm_type)
            case "exponential":
                ret = self.__scale_operator_exponential(number, tm_type)
            case "block":
                ret = self.__scale_operator_block(number, tm_type)
            case _:
                self.__log.warning(
                    f"[SCALING] Scaling method {scaling_method} not supported. Defaulting to linear."
                )
                ret = self.__scale_operator_linear(number, tm_type)
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
            ret = self.__scale_op(taskmanager)
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
        return first_node

    def run(self):
        node_name = self.__setup_run()
        if node_name == 1:
            return 1

        self.__log.info("[SCALING] First taskmanager, just waiting")
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
                current_state_taskmanagers = self.f.get_count_of_taskmanagers()
                self.__log.info(
                    f"[SCALING] Current statefulset taskmanagers: {current_state_taskmanagers}"
                )
                current_state_slots = self.f.get_total_slots()
                self.__log.info(
                    f"[SCALING] Current registered slots: {current_state_slots}"
                )

                # Expected state at the end of step
                expected_state_taskmanagers = self.steps[step]["taskmanager"]
                self.__log.info(
                    f"[SCALING] Expected statefulset taskmanagers: {expected_state_taskmanagers}"
                )
                expected_state_slots = sum(
                    [
                        taskmanager["number"]
                        for taskmanager in expected_state_taskmanagers
                    ]
                )
                self.__log.info(
                    f"[SCALING] Expected registered slots: {expected_state_slots}"
                )

                # Scale step
                ret = self.__scale_step(step)
                if ret == 1:
                    self.__log.info("[SCALING] Scaling is finishing due to stop event.")
                    return 1
                self.__log.info(
                    f"[SCALING] Scaling step on node {node_name} finished. Marking node as full."
                )
                self.k.node_manager.mark_node_as_full(node_name)

                current_state_taskmanagers = self.f.get_count_of_taskmanagers()
                self.__log.info(
                    f"[SCALING] Current statefulset taskmanagers: {current_state_taskmanagers}"
                )
                sleep(5)
        self.__log.info("[SCALING] Scaling finished.")
