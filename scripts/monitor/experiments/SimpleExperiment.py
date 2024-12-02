from datetime import datetime
from operator import indexOf
from time import sleep

from scripts.monitor.experiments.Experiment import Experiment
from scripts.src.data.DataExporter import DataExporter
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Tools import StoppableThread


class SimpleExperiment(Experiment):
    def __init__(self, log, config):
        super().__init__(log, config)
        self.log = log

        self.runs = self.config.get_int(Key.Experiment.runs)
        self.steps = self.config.get(Key.Experiment.Scaling.steps)
        self.log.info(
            f"[SIMPLE_E] SimpleExperiment initialized with runs: {self.runs}, steps: {self.steps}"
        )

        # Hold timestamps of all runs as a list of tuples (start, end)
        self.timestamps = []
        self.current_experiment_thread = None

    def start(self):
        # Check if chaos is enabled
        if self.is_chaos_enabled():
            self.log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )

        self.log.info("[SIMPLE_E] Starting experiment.")

        self.init_cluster()

    def stop(self):
        self.log.info("[SIMPLE_E] Stopping experiment.")
        if self.current_experiment_thread:
            self.current_experiment_thread.stop()
        for tuple in self.timestamps:
            try:
                # Iterate over the timestamps list and export data for each run
                start_ts, end_ts = tuple

                if start_ts is not None and end_ts is not None and end_ts > start_ts:
                    # Create experiment folder for results, ordered by date (YYYY-MM-DD)
                    exp_path = self.t.create_exp_folder(
                        self.EXPERIMENTS_BASE_PATH,
                        datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d"),
                    )

                    # Create log file with start timestamp
                    log_file = self.create_log_file(
                        exp_path=exp_path, start_ts=start_ts, end_ts=end_ts
                    )

                    # Add experiment run as header of log file
                    with open(log_file, "r+") as f:
                        content = f.read()
                        f.seek(0, 0)
                        f.write(
                            f"Experiment run {indexOf(self.timestamps, tuple) + 1}\n\n"
                            + content
                        )

                    # Export experiment data
                    data_exp: DataExporter = DataExporter(
                        log=self.log, exp_path=exp_path
                    )

                    # Export data from victoriametrics
                    data_exp.export()
                else:
                    self.log.error("[SIMPLE_E] Invalid timestamps. Skipping export.")

            except Exception as e:
                self.log.error(f"[SIMPLE_E] Error exporting data: {e}")

    def cleanup(self):
        try:
            # Remove SCHEDULABLE label from all nodes
            self.k.node_manager.reset_scaling_labels()
            self.k.node_manager.reset_state_labels()

            self.k.statefulset_manager.reset_taskmanagers()

            jobmanager_labels = "app=flink,component=jobmanager"
            self.k.pod_manager.delete_pods_by_label(jobmanager_labels, "flink")

            # delete load generators
            self.delete_load_generators()

        except Exception as e:
            self.log.error(f"[SIMPLE_E] Error cleaning up: {e}")

    def running(self):
        self.current_experiment_thread = StoppableThread(target=self._run_experiment)
        self.s.set_stopped_callback(self.current_experiment_thread.stopped)
        self.current_experiment_thread.start()
        # Wait for the thread to finish
        self.current_experiment_thread.join()

    def _run_experiment(self):
        run = 0
        while run < self.runs:
            self.log.info(f"[SIMPLE_E] Starting run {run + 1}")
            try:

                start_ts = int(datetime.now().timestamp())
                ret = self.single_run()
                if ret == 1:
                    self.log.info(f"[SIMPLE_E] Exiting run {run + 1}")
                    return
                end_ts = int(datetime.now().timestamp())

                # Save timestamps
                self.timestamps.append((start_ts, end_ts))
                # Cleanup after each run
                self.cleanup()

                self.log.info("[SIMPLE_E] Sleeping for 10 seconds before next run.")
                sleep(10)
                self.log.info(
                    f"[SIMPLE_E] Run {self.runs} completed. Start: {start_ts}, End: {end_ts}"
                )
            except Exception as e:
                self.log.error(f"[SIMPLE_E] Error during run: {e}")
                self.cleanup()

            run += 1

    def single_run(self):
        try:
            # Deploy load generators
            self.run_load_generators()
            # Deploy job
            self.f.run_job()
            # Run scaling steps on job
            ret = self.s.run()
            if ret == 1:
                return 1
        except Exception as e:
            self.log.error(f"[SIMPLE_E] Error during single run: {e}")
            self.cleanup()
