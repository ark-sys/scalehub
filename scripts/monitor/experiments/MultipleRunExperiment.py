from datetime import datetime
from operator import indexOf
from time import sleep

from scripts.monitor.experiments.Experiment import Experiment
from scripts.utils.DataExporter import DataExporter
from scripts.utils.Defaults import DefaultKeys as Key


class MultipleRunExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)
        self.log = log
        self.runs = self.config.get_int(Key.Experiment.runs)
        self.steps = self.config.get(Key.Experiment.Scaling.steps)
        # Hold timestamps of all runs as a list of tuples (start, end)
        self.timestamps = []

    def start(self):
        # Check if chaos is enabled
        if self.is_chaos_enabled():
            self.log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )

        self.log.info("Starting experiment.")

        # # #FOR TESTING Get current number of taskmanagers
        # taskmanagers_count_dict = {
        #     "flink-taskmanager-xxl": 0,
        #     "flink-taskmanager-xl": 0,
        #     "flink-taskmanager-l": 0,
        #     "flink-taskmanager-m": 0,
        #     "flink-taskmanager-s": 1,
        # }

    def stop(self):

        for run in range(self.runs):
            # Iterate over the timestamps list and export data for each run
            start_ts, end_ts = self.timestamps[run]
            # Create experiment folder for results, ordered by date (YYYY-MM-DD)
            exp_path = self.t.create_exp_folder(
                self.EXPERIMENTS_BASE_PATH,
                datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d"),
            )

            # Create log file with start timestamp
            log_file = self.create_log_file(
                exp_path=exp_path, start_ts=start_ts, end_ts=end_ts
            )
            # Export experiment data
            data_exp: DataExporter = DataExporter(log=self.__log, exp_path=exp_path)

            # Export data from victoriametrics
            data_exp.export()

            # if self.config.get_bool(Key.Experiment.output_stats):
            #     data_eval = DataEval(log=self.__log, exp_path=self.exp_path)
            #     # If output_stats is enabled, evaluate mean throughput and extract predictions from transscale-job logs in stats.csv file
            #     data_eval.eval_mean_stderr()
            #     # If output_plot is enabled, evaluate plot from stats.csv file
            #     if self.config.get_bool(Key.Experiment.output_plot):
            #         data_eval.eval_summary_plot()
            #         data_eval.eval_experiment_plot()
            #         data_eval.eval_plot_with_checkpoints()

    def cleanup(self):
        # Remove SCHEDULABLE label from all nodes
        self.k.node_manager.reset_scaling_labels()

        # Reload data-stream-apps
        self.p.run("data-stream-apps", config=self.config, tag="delete")
        sleep(5)
        self.p.run("data-stream-apps", config=self.config, tag="deploy")

        # delete load generators
        self.delete_load_generators()

    def running(self):
        run = 0
        while run < self.runs:
            self.log.info(f"Starting run {run}")
            start_ts = int(datetime.now().timestamp())

            self.single_run()
            end_ts = int(datetime.now().timestamp())
            # Save timestamps
            self.timestamps.append((start_ts, end_ts))
            sleep(5)

            # Cleanup after each run
            self.cleanup()

            self.log.info(
                f"Run {self.runs} completed. Start: {start_ts}, End: {end_ts}"
            )

            run += 1

    def initialize_cluster(self):
        # Check list of schedulable node, we should have 0
        schedulable_nodes = self.k.node_manager.get_schedulable_nodes()
        self.log.info(f"Schedulable nodes: {len(schedulable_nodes)}")
        # Check that we have the correct node labeled
        if len(schedulable_nodes) != 0:
            self.log.warning(f"Resetting scaling labels.")
            # Reset scaling labels, clean start.
            self.k.node_manager.reset_scaling_labels()

        # Get the first node to scale based on what's defined in the strategy file
        node_type = self.steps[0]["node"]
        if node_type == "vm_grid5000":
            vm_type = self.steps[0]["type"]
            first_node = self.k.node_manager.get_next_node(node_type, vm_type)
        else:
            first_node = self.k.node_manager.get_next_node(node_type)

        self.log.info(f"First node: {first_node}")

        # Mark this node with schedulable
        self.k.node_manager.mark_node_as_schedulable(first_node)

        # Get first taskmanager to deploy
        taskmanager_type = self.steps[0]["taskmanager"][0]["type"]

        # Get the name of the stateful set to scale
        tm_name = f"flink-taskmanager-{taskmanager_type}"

        # Scale up stateful set
        self.k.statefulset_manager.scale_statefulset(
            statefulset_name=tm_name, replicas=1
        )
        # Deploy load generators
        self.run_load_generators()
        # Deploy job
        self.run_job()

    def scale_operator(self, taskmanagers, step):

        for taskmanager in taskmanagers:
            self.log.info(
                f"Deploying {taskmanager['number']} taskmanagers of type {taskmanager['type']}"
            )
            # Get the number of taskmanagers to deploy
            tm_number = taskmanager["number"]
            # Get the type of taskmanager to deploy
            tm_type = taskmanager["type"]
            # Get the name of the stateful set to scale
            tm_name = f"flink-taskmanager-{tm_type}"

            # For each taskmanager to deploy, scale up stateful set, stop job with savepoint, rescale job from savepoint and sleep interval_s
            for i in range(tm_number):
                # if we are at the first step and first taskmanager, we don't need to scale up, just wait
                if step == 0 and i == 0 and indexOf(taskmanagers, taskmanager) == 0:
                    continue
                else:
                    # Get current number of taskmanagers
                    taskmanagers_count_dict = (
                        self.k.statefulset_manager.get_count_of_taskmanagers()
                    )
                    self.log.info(
                        f"Current number of taskmanagers: {taskmanagers_count_dict}"
                    )

                    # Scale up stateful set
                    taskmanagers_count_dict[tm_type] += 1
                    self.log.info(
                        f"Scaling up {tm_name} to {taskmanagers_count_dict[tm_type]}"
                    )
                    self.k.statefulset_manager.scale_statefulset(
                        statefulset_name=tm_name,
                        replicas=taskmanagers_count_dict[tm_type],
                    )

                    # Stop job with savepoint
                    job_id, operator_names, savepoint_path = self.stop_job()

                    if savepoint_path is None:
                        self.log.error("Savepoint failed.")
                        return
                    # Wait some time
                    sleep(5)

                    # Get new parallelism level as the sum of all taskmanagers
                    new_parallelism = sum(taskmanagers_count_dict.values())

                    # Rescale job from savepoint
                    self.rescale_job_from_savepoint(
                        operator_names, savepoint_path, new_parallelism
                    )
                    self.log.info(
                        f"Updated number of taskmanagers: {taskmanagers_count_dict}"
                    )
                self.log.info(
                    f"Waiting for {self.config.get_int(Key.Experiment.Scaling.interval_scaling_s)} seconds"
                )
                sleep(self.config.get_int(Key.Experiment.Scaling.interval_scaling_s))

    def single_run(self):
        steps = self.steps
        step = 0

        self.initialize_cluster()

        while step < len(steps):
            self.log.info(
                f"================================== Running step {step} =================================="
            )
            self.log.info(f"Step: {steps[step]}")

            # Get the number of taskmanagers to deploy at this step
            taskmanagers = steps[step]["taskmanager"]

            self.scale_operator(taskmanagers, step)

            step += 1
            sleep(3)
