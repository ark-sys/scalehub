# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from datetime import datetime
from time import sleep

from src.monitor.experiments.Experiment import Experiment
from src.utils.Defaults import DefaultKeys as Key


# This class needs to be fixed
@DeprecationWarning
class TransscaleExperiment(Experiment):
    exp_type = "transscale"

    def __init__(self, log, config):
        super().__init__(log, config)
        self.log = log
        self.start_ts = None
        self.end_ts = None

    def starting(self):
        # Get start timestamp
        self.start_ts = int(datetime.now().timestamp())

        # Check if chaos is enabled
        if self.config.get_bool(Key.Experiment.Chaos.enable.key):
            self.__log.info(
                "Chaos injection enabled. Deploying chaos resources on Consul and Flink."
            )
            # Setup experiment_params
            chaos_params = {
                "affected_nodes_percentage": self.config.get_int(
                    Key.Experiment.Chaos.affected_nodes_percentage.key
                ),
                "latency": self.config.get_int(Key.Experiment.Chaos.delay_latency_ms.key),
                "jitter": self.config.get_int(Key.Experiment.Chaos.delay_jitter_ms.key),
                "correlation": self.config.get_float(Key.Experiment.Chaos.delay_correlation.key),
                "rate": self.config.get_int(Key.Experiment.Chaos.bandwidth_rate_mbps.key),
                "limit": self.config.get_int(Key.Experiment.Chaos.bandwidth_limit.key),
                "buffer": self.config.get_int(Key.Experiment.Chaos.bandwidth_buffer.key),
            }
            self.k.chaos_manager.deploy_networkchaos(chaos_params)

        # # Reset nodes labels
        # self.__log.info("Resetting nodes labels.")
        #
        # # remove label "node-role.kubernetes.io/scaling" from all nodes
        # autoscaling_label = "node-role.kubernetes.io/scaling"
        # self.k.node_manager.remove_label_from_nodes(
        #     worker_nodes, f"{autoscaling_label}=SCHEDULABLE"
        # )
        # self.k.node_manager.remove_label_from_nodes(
        #     worker_nodes, f"{autoscaling_label}=UNSCHEDULABLE"
        # )
        # # Get clean nodes (worker_nodes - impacted_nodes)
        # clean_nodes = list(set(worker_nodes) - set(impacted_nodes))
        # # Add label "node-role.kubernetes.io/scaling" to clean nodes
        # self.k.node_manager.add_label_to_nodes(
        #     clean_nodes, f"{autoscaling_label}=SCHEDULABLE"
        # )

        # Reset taskmanager replicas
        self.__log.info("Resetting taskmanager replicas.")
        # Reset to 0 and back to 1 to trigger placement of taskmanager on schedulable nodes
        self.k.deployment_manager.scale_deployment(
            "flink-taskmanager", replicas=0, namespace="flink"
        )
        sleep(1)
        self.k.deployment_manager.scale_deployment(
            "flink-taskmanager", replicas=1, namespace="flink"
        )

        # Deploy load generators
        # self.run_load_generators()

        # Deploy flink job
        self.f.run_job()
        # # Resume execution of load generators
        # self.k.pod_manager.execute_command_on_pods_by_label(
        #     "type=load-generator", command="touch /start_generation"
        # )

        # Retrieve resource definition for transscale-job
        transscale_resource_definition = self.k.get_configmap("transscale-job-definition")

        # Run transscale-job
        self.k.job_manager.create_job(transscale_resource_definition["transscale-job.yaml"])

    # def finishing(self):
    # self.end_ts = int(datetime.now().timestamp())
    # # Create experiment folder for results, ordered by date (YYYY-MM-DD)
    # # self.exp_path = self.t.create_exp_folder(
    # #     self.EXPERIMENTS_BASE_PATH,
    # #     datetime.fromtimestamp(self.start_ts).strftime("%Y-%m-%d"),
    # # )
    # f: FolderManager = FolderManager(self.__log, self.EXPERIMENTS_BASE_PATH)
    # data_path = f.create_date_folder(self.start_ts)
    # self.exp_path = f.create_subfolder(data_path)
    #
    # # Create log file with start timestamp
    # self.log_file = self.create_log_file(
    #     exp_path=self.exp_path, start_ts=self.start_ts, end_ts=self.end_ts
    # )
    #
    # # Get logs from transscale-job
    # transscale_logs = self.k.job_manager.get_job_logs("transscale-job", "default")
    #
    # # Save transscale-job logs
    # with open(os.path.join(self.exp_path, "transscale_log.txt"), "w") as file:
    #     file.write(transscale_logs)
    #
    # # Export experiment data
    # data_exp: DataExporter = DataExporter(log=self.__log, exp_path=self.exp_path)
    #
    # # Export data from victoriametrics
    # data_exp.export()
    #
    # if self.config.get_bool(Key.Experiment.output_stats.key):
    #     data_eval = DataEval(log=self.__log, exp_path=self.exp_path)
    #     # If output_stats is enabled, evaluate mean throughput and extract predictions from transscale-job logs in stats.csv file
    #     data_eval.eval_mean_stderr()
    #     # If output_plot is enabled, evaluate plot from stats.csv file
    #     if self.config.get_bool(Key.Experiment.output_plot.key):
    #         data_eval.eval_summary_plot()
    #         data_eval.eval_experiment_plot()
    #         data_eval.eval_plot_with_checkpoints()

    def cleaning(self):
        self.k.pod_manager.execute_command_on_pod(
            deployment_name="flink-jobmanager",
            command="for job_id in $(flink list -r | awk -F ' : ' ' {print $2}'); do flink cancel $job_id ;done",
        )

        # Scale down taskmanagers
        self.k.deployment_manager.scale_deployment(
            "flink-taskmanager", replicas=1, namespace="flink"
        )
        # Clean transscale job
        self.k.job_manager.delete_job("transscale-job")
        # Clean transscale remaining pods
        self.k.pod_manager.delete_pods_by_label("job-name=transscale-job")
        # Delete load generators
        for generator in self.config.get(Key.Experiment.Generators.generators.key):
            load_generator_params = {
                "lg_name": generator["name"],
                "lg_topic": generator["topic"],
                "lg_numsensors": int(generator["num_sensors"]),
                "lg_intervalms": int(generator["interval_ms"]),
                "lg_replicas": int(generator["replicas"]),
                "lg_value": int(generator["value"]),
            }
            self.k.service_manager.delete_service_from_template(
                self.load_generator_service_template, load_generator_params
            )
            self.k.deployment_manager.delete_deployment_from_template(
                self.load_generator_deployment_template, load_generator_params
            )

        if self.config.get_bool(Key.Experiment.Chaos.enable.key):
            # Clean all network chaos resources
            self.k.chaos_manager.delete_networkchaos()

        # Clean variables
        self.start_ts = None
        self.end_ts = None
        self.exp_path = None
        self.config = None

        self.__log.info("Cleaning complete.")

    def running(self):
        self.log.info("Running autoscaling experiment.")

        while True:
            sleep(1)
            try:
                job_status = self.k.job_manager.get_job_status("transscale-job")
                if not job_status:
                    continue
                else:
                    if job_status[0].type == "Complete":
                        break
            except Exception as e:
                self.__log.warning(f"Error while getting job status: {str(e)}")
