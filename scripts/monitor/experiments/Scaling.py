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

        # Set sleep command
        self.__sleep = None

    def set_sleep_command(self, sleep):
        self.__sleep = sleep

    def __scale_and_wait(self, replicas):
        self.__log.info(
            f"[SCALING] ************************************ Adding {replicas} replicas ************************************"
        )
        ret = self.f.run_job(new_parallelism=replicas)
        if ret == 1:
            return 1
        else:
            # Rescale successful, populate job info
            self.f.get_job_info()
            self.f.check_nominal_job_run()
            ret = self.f.wait_for_job_running()
            if ret == 1:
                return 1
            self.__log.info(
                f"[SCALING] Monitoring interval: for {self.interval_scaling_s} seconds"
            )
            return self.__sleep(self.interval_scaling_s)

    def __scale_w_tm(self, replicas, tm_type):
        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{tm_type}"

        # Get current number of taskmanagers
        taskmanagers_count_dict = self.k.statefulset_manager.get_count_of_taskmanagers()
        # Scale up stateful set
        new_tm_count = taskmanagers_count_dict[tm_type] + replicas
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name,
            replicas=new_tm_count,
            namespace="flink",
        )

        ret = self.__scale_and_wait(new_tm_count)
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
            val = 1
            while sum(pascaline_sequence) < seq_n:
                pascaline_sequence.append(2**val)
                val += 1
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
                ret = self.__scale_linear(number, tm_type, scope)
        if ret == 1:
            return 1

    def __scale_step(self, step):
        self.__log.info(
            f"=========================================================== Step {step} ==========================================================="
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
                self.__log.info(f"[SCALING] Next node: {next_node}\n")
                self.k.node_manager.mark_node_as_schedulable(next_node)
                return next_node, "pass"
            else:
                self.__log.error("[SCALING] No more nodes available.\n")
                return None, "break"
        # Handle first step case
        elif step == 0:
            # If there is only one taskmanager in the list, check count is 1 or method is block
            if len(self.steps[step]["taskmanager"]) == 1:
                # Check count and method
                if (
                    self.steps[step]["taskmanager"][0]["number"] == 1
                    or self.steps[step]["taskmanager"][0]["method"] == "block"
                ):
                    self.__log.info(
                        "[SCALING] First node and first taskmanager already scaled, mark node as full. Continue to next step.\n"
                    )
                    self.k.node_manager.mark_node_as_full(node_name)
                    return node_name, "continue"
                else:
                    # If count is more than 1 and method is not block, decrement count as one taskmanager is already scaled during setup
                    self.steps[step]["taskmanager"][0]["number"] -= 1
            # If there are more than one taskmanagers in the list, check if count is 1 or method is block and remove the first taskmanager
            else:
                if (
                    self.steps[step]["taskmanager"][0]["number"] == 1
                    or self.steps[step]["taskmanager"][0]["method"] == "block"
                ):
                    self.__log.info(
                        "[SCALING] First taskmanager already scaled. Removing from list and resuming current iteration.\n"
                    )
                    self.steps[step]["taskmanager"].pop(0)
            return node_name, "pass"
        else:
            self.__log.error(
                "[SCALING] What is happening? i can't be less than 0. Breaking.\n"
            )
            return node_name, "break"

    def __setup_run(self):
        self.__log.info("[SCALING] Setting up experiment.\n\n")
        ######################################## Prepare cluster for scaling ########################################
        # Reset scaling labels, clean start.
        self.k.node_manager.reset_scaling_labels()
        # Reset state labels
        self.k.node_manager.reset_state_labels()

        ######################################## Mark first node as schedulable ########################################
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

        self.__log.info(f"[SCALING] First node: {first_node}\n")

        # Mark this node with schedulable
        self.k.node_manager.mark_node_as_schedulable(first_node)

        ######################################## Scale first taskmanager ########################################
        # Get first taskmanager to deploy
        taskmanager_type = self.steps[0]["taskmanager"][0]["type"]
        taskmanager_method = self.steps[0]["taskmanager"][0]["method"]
        taskmanager_number = self.steps[0]["taskmanager"][0]["number"]
        taskmanager_scope = (
            self.steps[0]["taskmanager"][0]["scope"]
            if "scope" in self.steps[0]["taskmanager"][0]
            else "taskmanager"
        )
        tm_name = f"flink-taskmanager-{taskmanager_type}"

        # If method is block, scale up taskmanagers at once
        if taskmanager_method == "block" and taskmanager_scope == "taskmanager":
            self.__log.debug(
                f"[SCALING] Block method on taskmanagers detected. Scaling {taskmanager_number} taskmanagers at once"
            )
            # Scale up stateful set
            self.k.statefulset_manager.scale_statefulset(
                statefulset_name=tm_name, replicas=taskmanager_number, namespace="flink"
            )

            # Start the job
            ret = self.f.run_job(start_par=taskmanager_number)
        else:
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

        self.__log.info("[SCALING] First iteration after setup, just waiting...")
        ret = self.__sleep(self.interval_scaling_s)
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
                self.__sleep(5)
        self.__log.info("[SCALING] Scaling finished.")
