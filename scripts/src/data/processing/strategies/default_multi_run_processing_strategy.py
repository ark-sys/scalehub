import json
from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd

from scripts.src.data.loading.loader import Loader
from scripts.src.data.loading.strategies.victoria_metrics_load_strategy import (
    VictoriaMetricsLoadStrategy,
)
from scripts.src.data.processing.metrics_processor import MetricsProcessor
from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)
from scripts.utils.Defaults import DefaultKeys as Key


class DefaultMultiRunProcessingStrategy(BaseProcessingStrategy):
    """
    Default strategy for processing multi-run experiments when no specific type is detected.
    This strategy loads data from VictoriaMetrics for each run, processes it, and aggregates results.
    """

    def __init__(self, logger, exp_path: Path, config):
        super().__init__(logger, exp_path)
        self.config = config
        self.start_skip = config.get_int(Key.Experiment.output_skip_s.key, 60)
        self.end_skip = 30

    def process(self) -> Dict[str, Any]:
        """Main processing workflow for default multi-run experiments."""
        self.logger.info("Processing with DefaultMultiRunProcessingStrategy...")

        # Find all run subdirectories
        run_dirs = self._get_run_directories()
        if not run_dirs:
            self.logger.error("No run directories found")
            return {
                "type": "default_multi_run",
                "status": "error",
                "message": "No runs found",
            }

        self.logger.info(f"Found {len(run_dirs)} runs to process")

        # Process each run
        all_runs_data = []
        for run_dir in run_dirs:
            try:
                run_data = self._process_single_run(run_dir)
                if run_data is not None:
                    all_runs_data.append(run_data)
            except Exception as e:
                self.logger.error(f"Error processing run {run_dir.name}: {e}")

        if not all_runs_data:
            self.logger.error("No runs were successfully processed")
            return {
                "type": "default_multi_run",
                "status": "error",
                "message": "All runs failed",
            }

        self.logger.info(f"Successfully processed {len(all_runs_data)} runs")

        # Aggregate results across runs to create final_df.csv
        aggregated_results = self._aggregate_runs(all_runs_data)

        # Export aggregated results as final_df.csv (multi-run level)
        final_df_path = self.exp_path / "final_df.csv"
        self.exporter.export_data(aggregated_results, final_df_path)
        self.logger.info(f"Multi-run final_df.csv saved to: {final_df_path}")

        # Also export as aggregated_results.csv for backward compatibility
        output_path = self.exp_path / "aggregated_results.csv"
        self.exporter.export_data(aggregated_results, output_path)

        # Generate summary plots with aggregated data per parallelism level
        self._generate_summary_plots(all_runs_data, aggregated_results)

        return {
            "type": "default_multi_run",
            "status": "success",
            "runs_processed": len(all_runs_data),
            "output_path": str(final_df_path),
        }

    def _get_run_directories(self) -> list[Path]:
        """Get all run directories (directories with numeric names)."""
        run_dirs = []
        for item in self.exp_path.iterdir():
            if item.is_dir() and item.name.isdigit():
                run_dirs.append(item)
        return sorted(run_dirs, key=lambda x: int(x.name))

    def _process_single_run(self, run_dir: Path) -> Optional[pd.DataFrame]:
        """Process a single run directory."""
        self.logger.info(f"Processing run: {run_dir.name}")

        # Load exp_log.json to get timestamps and db_url
        exp_log_path = run_dir / "exp_log.json"
        if not exp_log_path.exists():
            self.logger.warning(f"No exp_log.json found in {run_dir.name}")
            return None

        with open(exp_log_path, "r") as f:
            exp_log = json.load(f)

        timestamps = exp_log.get("timestamps", {})
        start_ts = timestamps.get("start")
        end_ts = timestamps.get("end")

        if not start_ts or not end_ts:
            self.logger.warning(f"Missing timestamps in {run_dir.name}")
            return None

        # Get database URL from config
        db_url = self._get_db_url()

        # Load data from VictoriaMetrics in CSV format
        vm_strategy_csv = VictoriaMetricsLoadStrategy(
            self.logger, db_url, str(start_ts), str(end_ts)
        )
        vm_loader_csv = Loader(vm_strategy_csv)
        raw_data_csv = vm_loader_csv.load_data(format="csv")

        # Load data from VictoriaMetrics in JSON format for detailed processing
        vm_strategy_json = VictoriaMetricsLoadStrategy(
            self.logger, db_url, str(start_ts), str(end_ts)
        )
        vm_loader_json = Loader(vm_strategy_json)
        raw_data_json = vm_loader_json.load_data(format="json")

        if not raw_data_csv and not raw_data_json:
            self.logger.warning(
                f"No data loaded from VictoriaMetrics for run {run_dir.name}"
            )
            return None

        # Export raw metrics to CSV and JSON files (like old DataExporter)
        self._export_raw_metrics(raw_data_csv, raw_data_json, run_dir)

        # Build final_df.csv with multi-column structure
        self._build_final_dataframe(raw_data_json, run_dir, exp_log)

        # Process the run using SingleExperimentProcessor to generate mean_stderr.csv and plots
        final_df_path = run_dir / "final_df.csv"
        if final_df_path.exists():
            try:
                from scripts.src.data.processing.single_experiment_processor import (
                    SingleExperimentProcessor,
                )

                single_processor = SingleExperimentProcessor(
                    self.logger, self.config, str(run_dir)
                )
                single_processor.process()
                self.logger.info(
                    f"Generated plots and statistics for run {run_dir.name}"
                )

                # Load the generated mean_stderr.csv for aggregation
                mean_stderr_path = run_dir / "mean_stderr.csv"
                if mean_stderr_path.exists():
                    return pd.read_csv(mean_stderr_path, index_col=0)

            except Exception as e:
                self.logger.error(
                    f"Failed to process single experiment for run {run_dir.name}: {e}"
                )

        return None

    def _export_raw_metrics(
        self,
        raw_data_csv: Dict[str, pd.DataFrame],
        raw_data_json: Dict[str, list],
        run_dir: Path,
    ) -> None:
        """Export raw metrics to CSV and JSON files for each metric."""
        export_dir = run_dir / "export"
        export_dir.mkdir(exist_ok=True)

        # Export CSV files
        if raw_data_csv:
            for metric_name, df in raw_data_csv.items():
                if df is not None and not df.empty:
                    output_path = export_dir / f"{metric_name}_export.csv"
                    df.to_csv(output_path)
                    self.logger.debug(
                        f"Exported raw metric {metric_name} to {output_path}"
                    )

        # Export JSON files
        if raw_data_json:
            for metric_name, json_data in raw_data_json.items():
                if json_data:
                    output_path = export_dir / f"{metric_name}_export.json"
                    with open(output_path, "w") as f:
                        for item in json_data:
                            f.write(json.dumps(item) + "\n")
                    self.logger.debug(
                        f"Exported raw metric {metric_name} to {output_path}"
                    )

    def _build_final_dataframe(
        self, raw_data_json: Dict[str, list], run_dir: Path, exp_log: dict
    ) -> None:
        """Build final_df.csv with multi-column structure from JSON data."""
        if not raw_data_json:
            self.logger.warning("No JSON data available to build final_df")
            return

        try:
            metrics_processor = MetricsProcessor(self.logger)

            # Get experiment configuration
            config_str = exp_log.get("config", "{}")
            if isinstance(config_str, str):
                config_dict = json.loads(config_str)
            else:
                config_dict = config_str

            # Get operator/task name
            task_name = config_dict.get("experiment.task_name", "Unknown")
            if task_name == "TumblingEventTimeWindows":
                task_name = "TumblingEventTimeWindows____Timestamps_Watermarks"

            # Process operator metrics (task-specific metrics)
            operator_metrics_list = []
            operator_metrics = [
                "flink_taskmanager_job_task_numRecordsInPerSecond",
                "flink_taskmanager_job_task_busyTimeMsPerSecond",
                "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond",
            ]

            for metric_name in operator_metrics:
                if metric_name in raw_data_json:
                    df = metrics_processor.get_metrics_per_subtask(
                        raw_data_json[metric_name], metric_name, task_name
                    )
                    if not df.empty:
                        operator_metrics_list.append(df)

            # Process source metrics
            sources_metrics_list = []
            source_metrics = ["flink_taskmanager_job_task_numRecordsInPerSecond"]

            for metric_name in source_metrics:
                if metric_name in raw_data_json:
                    dfs = metrics_processor.get_sources_metrics(
                        raw_data_json[metric_name], metric_name
                    )
                    sources_metrics_list.extend(dfs)

            # Build final dataframe
            final_df = metrics_processor.build_final_dataframe(
                operator_metrics_list, sources_metrics_list
            )

            if not final_df.empty:
                output_path = run_dir / "final_df.csv"
                final_df.to_csv(output_path)
                self.logger.info(f"Built final_df.csv with shape {final_df.shape}")
            else:
                self.logger.warning("Final DataFrame is empty, skipping export")

        except Exception as e:
            self.logger.error(f"Failed to build final_df: {e}")

    def _get_db_url(self) -> str:
        """Get VictoriaMetrics URL from config or use default."""
        # Try to get from config, otherwise use default in-cluster URL
        try:
            db_url = self.config.get_str(
                "experiment.db_url",
                "victoria-metrics.monitoring.svc.cluster.local:8428",
            )
        except:
            db_url = "victoria-metrics.monitoring.svc.cluster.local:8428"
        return db_url

    def _calculate_run_statistics(
        self, raw_data: Dict[str, pd.DataFrame], run_name: str
    ) -> pd.DataFrame:
        """Calculate statistics for a single run from raw time series data."""
        stats = {}

        # Process throughput
        if "flink_taskmanager_job_task_numRecordsInPerSecond" in raw_data:
            throughput_df = raw_data["flink_taskmanager_job_task_numRecordsInPerSecond"]
            filtered_throughput = self._filter_timeseries(throughput_df)
            if len(filtered_throughput) > 0:
                stats["Throughput_mean"] = filtered_throughput["Value"].mean()
                stats["Throughput_std"] = filtered_throughput["Value"].std()
                stats["Throughput_min"] = filtered_throughput["Value"].min()
                stats["Throughput_max"] = filtered_throughput["Value"].max()
                stats["Throughput_stderr"] = filtered_throughput[
                    "Value"
                ].std() / np.sqrt(len(filtered_throughput))

        # Process busy time
        if "flink_taskmanager_job_task_busyTimeMsPerSecond" in raw_data:
            busy_df = raw_data["flink_taskmanager_job_task_busyTimeMsPerSecond"]
            filtered_busy = self._filter_timeseries(busy_df)
            if len(filtered_busy) > 0:
                stats["BusyTime_mean"] = filtered_busy["Value"].mean()
                stats["BusyTime_std"] = filtered_busy["Value"].std()
                stats["BusyTime_stderr"] = filtered_busy["Value"].std() / np.sqrt(
                    len(filtered_busy)
                )

        # Process backpressure time
        if "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond" in raw_data:
            bp_df = raw_data[
                "flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"
            ]
            filtered_bp = self._filter_timeseries(bp_df)
            if len(filtered_bp) > 0:
                stats["BackpressureTime_mean"] = filtered_bp["Value"].mean()
                stats["BackpressureTime_std"] = filtered_bp["Value"].std()
                stats["BackpressureTime_stderr"] = filtered_bp["Value"].std() / np.sqrt(
                    len(filtered_bp)
                )

        # Process checkpoint duration (JobManager metric)
        if "flink_jobmanager_job_lastCheckpointDuration" in raw_data:
            checkpoint_df = raw_data["flink_jobmanager_job_lastCheckpointDuration"]
            filtered_checkpoint = self._filter_timeseries(checkpoint_df)
            if len(filtered_checkpoint) > 0:
                stats["CheckpointDuration_mean"] = filtered_checkpoint["Value"].mean()
                stats["CheckpointDuration_std"] = filtered_checkpoint["Value"].std()
                stats["CheckpointDuration_max"] = filtered_checkpoint["Value"].max()

        # Process checkpoint alignment time
        if "flink_taskmanager_job_task_checkpointStartDelayNanos" in raw_data:
            alignment_df = raw_data[
                "flink_taskmanager_job_task_checkpointStartDelayNanos"
            ]
            filtered_alignment = self._filter_timeseries(alignment_df)
            if len(filtered_alignment) > 0:
                # Convert from nanoseconds to milliseconds
                stats["CheckpointAlignment_mean"] = (
                    filtered_alignment["Value"].mean() / 1_000_000
                )
                stats["CheckpointAlignment_std"] = (
                    filtered_alignment["Value"].std() / 1_000_000
                )
                stats["CheckpointAlignment_max"] = (
                    filtered_alignment["Value"].max() / 1_000_000
                )

        # Process late records dropped
        if "flink_taskmanager_job_task_operator_numLateRecordsDropped" in raw_data:
            late_df = raw_data[
                "flink_taskmanager_job_task_operator_numLateRecordsDropped"
            ]
            filtered_late = self._filter_timeseries(late_df)
            if len(filtered_late) > 0:
                stats["LateRecordsDropped_total"] = filtered_late["Value"].sum()
                stats["LateRecordsDropped_mean"] = filtered_late["Value"].mean()

        # Process CPU load
        if "flink_taskmanager_Status_JVM_CPU_Load" in raw_data:
            cpu_df = raw_data["flink_taskmanager_Status_JVM_CPU_Load"]
            filtered_cpu = self._filter_timeseries(cpu_df)
            if len(filtered_cpu) > 0:
                stats["CPU_Load_mean"] = filtered_cpu["Value"].mean()
                stats["CPU_Load_std"] = filtered_cpu["Value"].std()

        # Process CPU time
        if "flink_taskmanager_Status_JVM_CPU_Time" in raw_data:
            cpu_time_df = raw_data["flink_taskmanager_Status_JVM_CPU_Time"]
            filtered_cpu_time = self._filter_timeseries(cpu_time_df)
            if len(filtered_cpu_time) > 0:
                stats["CPU_Time_mean"] = filtered_cpu_time["Value"].mean()
                stats["CPU_Time_std"] = filtered_cpu_time["Value"].std()

        # Process CPU threads
        if "flink_taskmanager_Status_JVM_Threads_Count" in raw_data:
            threads_df = raw_data["flink_taskmanager_Status_JVM_Threads_Count"]
            filtered_threads = self._filter_timeseries(threads_df)
            if len(filtered_threads) > 0:
                stats["CPU_Threads_mean"] = filtered_threads["Value"].mean()
                stats["CPU_Threads_std"] = filtered_threads["Value"].std()

        # Process memory usage (heap)
        if "flink_taskmanager_Status_JVM_Memory_Heap_Used" in raw_data:
            mem_df = raw_data["flink_taskmanager_Status_JVM_Memory_Heap_Used"]
            filtered_mem = self._filter_timeseries(mem_df)
            if len(filtered_mem) > 0:
                stats["Memory_Heap_mean"] = filtered_mem["Value"].mean()
                stats["Memory_Heap_std"] = filtered_mem["Value"].std()

        # Process memory usage (managed)
        if "flink_taskmanager_Status_Flink_Memory_Managed_Used" in raw_data:
            managed_mem_df = raw_data[
                "flink_taskmanager_Status_Flink_Memory_Managed_Used"
            ]
            filtered_managed_mem = self._filter_timeseries(managed_mem_df)
            if len(filtered_managed_mem) > 0:
                stats["Memory_Managed_mean"] = filtered_managed_mem["Value"].mean()
                stats["Memory_Managed_std"] = filtered_managed_mem["Value"].std()

        # Process GC time
        if (
            "flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time"
            in raw_data
        ):
            gc_df = raw_data[
                "flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time"
            ]
            filtered_gc = self._filter_timeseries(gc_df)
            if len(filtered_gc) > 0:
                stats["GC_Time_mean"] = filtered_gc["Value"].mean()
                stats["GC_Time_std"] = filtered_gc["Value"].std()

        # Create DataFrame with run identifier
        stats["Run"] = run_name
        return pd.DataFrame([stats])

    def _filter_timeseries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter time series data based on start and end skip times."""
        if df.empty:
            return df

        min_ts = df.index.min()
        max_ts = df.index.max()

        filtered = df[
            (df.index >= min_ts + self.start_skip)
            & (df.index <= max_ts - self.end_skip)
        ]

        return filtered

    def _aggregate_runs(self, all_runs_data: list[pd.DataFrame]) -> pd.DataFrame:
        """Aggregate statistics across all runs to create final_df.csv for multi-run."""
        if not all_runs_data:
            return pd.DataFrame()

        # Combine all mean_stderr.csv data
        combined_df = pd.concat(all_runs_data, keys=range(1, len(all_runs_data) + 1))
        combined_df.index.names = ["Run", "Parallelism"]

        # Calculate statistics across runs for each parallelism level
        final_df = combined_df.groupby("Parallelism").agg(
            {
                "Throughput": ["mean", "std", "min", "max"],
                "ThroughputStdErr": "mean",
                "BusyTime": ["mean", "std"],
                "BusyTimeStdErr": "mean",
                "BackpressureTime": ["mean", "std"],
                "BackpressureTimeStdErr": "mean",
            }
        )

        # Flatten multi-level columns
        final_df.columns = ["_".join(col).strip("_") for col in final_df.columns.values]

        # Rename columns to match expected format
        final_df = final_df.rename(
            columns={
                "Throughput_mean": "Throughput",
                "Throughput_std": "Throughput_std",
                "Throughput_min": "Throughput_min",
                "Throughput_max": "Throughput_max",
                "ThroughputStdErr_mean": "ThroughputStdErr",
                "BusyTime_mean": "BusyTime",
                "BusyTime_std": "BusyTime_std",
                "BusyTimeStdErr_mean": "BusyTimeStdErr",
                "BackpressureTime_mean": "BackpressureTime",
                "BackpressureTime_std": "BackpressureTime_std",
                "BackpressureTimeStdErr_mean": "BackpressureTimeStdErr",
            }
        )

        return final_df

    def _generate_summary_plots(
        self, all_runs_data: list[pd.DataFrame], aggregated_results: pd.DataFrame
    ) -> None:
        """Generate summary plots for the multi-run experiment."""
        self.logger.info("Generating summary plots...")

        if aggregated_results.empty:
            self.logger.warning("No aggregated results to plot")
            return

        # Plot throughput with min/max range across runs
        if "Throughput" in aggregated_results.columns:
            try:
                parallelism_levels = aggregated_results.index.tolist()
                throughput_mean = aggregated_results["Throughput"].tolist()
                throughput_min = aggregated_results.get(
                    "Throughput_min", throughput_mean
                )
                throughput_max = aggregated_results.get(
                    "Throughput_max", throughput_mean
                )

                # Create error bars from min/max
                yerr_lower = [
                    mean - min_val
                    for mean, min_val in zip(throughput_mean, throughput_min)
                ]
                yerr_upper = [
                    max_val - mean
                    for mean, max_val in zip(throughput_mean, throughput_max)
                ]

                # Calculate ylim: 0 to max_value + 50k
                max_value = max(throughput_max)
                ylim_max = max_value + 50000

                self.plotter.generate_plot(
                    {
                        "x": parallelism_levels,
                        "y": throughput_mean,
                        "yerr": [yerr_lower, yerr_upper],
                    },
                    plot_type="basic",
                    xlabel="Parallelism",
                    ylabel="Throughput (records/s)",
                    title="Throughput Across Runs",
                    ylim=(0, ylim_max),
                    filename="throughput_across_runs.png",
                )
            except Exception as e:
                self.logger.error(f"Failed to generate throughput plot: {e}")

        # Plot busy time and backpressure time with fixed y-scale
        if (
            "BusyTime" in aggregated_results.columns
            and "BackpressureTime" in aggregated_results.columns
        ):
            try:
                parallelism_levels = aggregated_results.index.tolist()

                # Create separate series for each metric
                busy_time = aggregated_results["BusyTime"]
                backpressure_time = aggregated_results["BackpressureTime"]

                # Create DataFrame for stacked plot
                plot_df = pd.DataFrame(
                    {
                        "BusyTime": busy_time,
                        "BackpressureTime": backpressure_time,
                    },
                    index=parallelism_levels,
                )

                self.plotter.generate_plot(
                    {
                        "BusyTime": plot_df["BusyTime"],
                        "BackpressureTime": plot_df["BackpressureTime"],
                    },
                    plot_type="stacked",
                    xlabel="Parallelism",
                    ylabels_dict={
                        "BusyTime": "ms/s",
                        "BackpressureTime": "ms/s",
                    },
                    ylim_dict={
                        "BusyTime": (0, 1100),
                        "BackpressureTime": (0, 1100),
                    },
                    title="Task Time Metrics Across Runs",
                    filename="time_metrics_across_runs.png",
                )
            except Exception as e:
                self.logger.error(f"Failed to generate time metrics plot: {e}")

        # Plot checkpointing metrics
        self._plot_checkpoint_metrics(aggregated_results)

        # Plot resource usage metrics
        self._plot_resource_metrics(aggregated_results)

        # Plot late records dropped
        self._plot_late_records(aggregated_results)

        self.logger.info("Summary plots generated successfully")

    def _plot_checkpoint_metrics(self, aggregated_results: pd.DataFrame) -> None:
        """Plot checkpointing-related metrics."""
        checkpoint_cols = [
            col for col in aggregated_results.columns if "Checkpoint" in col
        ]

        if not checkpoint_cols:
            return

        try:
            parallelism_levels = aggregated_results.index.tolist()
            plot_data = {}

            # Checkpoint duration
            if "CheckpointDuration_mean" in aggregated_results.columns:
                plot_data["Checkpoint Duration"] = aggregated_results[
                    "CheckpointDuration_mean"
                ]

            # Checkpoint alignment time
            if "CheckpointAlignment_mean" in aggregated_results.columns:
                plot_data["Checkpoint Alignment"] = aggregated_results[
                    "CheckpointAlignment_mean"
                ]

            if plot_data:
                plot_df = pd.DataFrame(plot_data, index=parallelism_levels)

                self.plotter.generate_plot(
                    {
                        "Checkpoint Duration": plot_df.get("Checkpoint Duration"),
                        "Checkpoint Alignment": plot_df.get("Checkpoint Alignment"),
                    },
                    plot_type="stacked",
                    xlabel="Parallelism",
                    ylabels_dict={
                        "Checkpoint Duration": "ms",
                        "Checkpoint Alignment": "ms",
                    },
                    title="Checkpointing Metrics Across Runs",
                    filename="checkpoint_metrics.png",
                )
                self.logger.info("Generated checkpoint metrics plot")
        except Exception as e:
            self.logger.error(f"Failed to generate checkpoint metrics plot: {e}")

    def _plot_resource_metrics(self, aggregated_results: pd.DataFrame) -> None:
        """Plot resource usage metrics (CPU, Memory, GC)."""
        resource_cols = [
            col
            for col in aggregated_results.columns
            if any(x in col for x in ["CPU", "Memory", "GC"])
        ]

        if not resource_cols:
            return

        try:
            parallelism_levels = aggregated_results.index.tolist()

            # Plot CPU metrics
            cpu_plot_data = {}
            if "CPU_Load_mean" in aggregated_results.columns:
                cpu_plot_data["CPU Load"] = aggregated_results["CPU_Load_mean"]
            if "CPU_Time_mean" in aggregated_results.columns:
                cpu_plot_data["CPU Time"] = aggregated_results["CPU_Time_mean"]
            if "CPU_Threads_mean" in aggregated_results.columns:
                cpu_plot_data["CPU Threads"] = aggregated_results["CPU_Threads_mean"]

            if cpu_plot_data:
                cpu_df = pd.DataFrame(cpu_plot_data, index=parallelism_levels)
                self.plotter.generate_plot(
                    {k: v for k, v in cpu_df.items()},
                    plot_type="stacked",
                    xlabel="Parallelism",
                    ylabels_dict={k: "Value" for k in cpu_plot_data.keys()},
                    title="CPU Metrics Across Runs",
                    filename="cpu_metrics.png",
                )
                self.logger.info("Generated CPU metrics plot")

            # Plot Memory metrics
            memory_plot_data = {}
            if "Memory_Heap_mean" in aggregated_results.columns:
                memory_plot_data["Heap Memory"] = aggregated_results["Memory_Heap_mean"]
            if "Memory_Managed_mean" in aggregated_results.columns:
                memory_plot_data["Managed Memory"] = aggregated_results[
                    "Memory_Managed_mean"
                ]

            if memory_plot_data:
                memory_df = pd.DataFrame(memory_plot_data, index=parallelism_levels)
                self.plotter.generate_plot(
                    {k: v for k, v in memory_df.items()},
                    plot_type="stacked",
                    xlabel="Parallelism",
                    ylabels_dict={k: "Bytes" for k in memory_plot_data.keys()},
                    title="Memory Usage Across Runs",
                    filename="memory_metrics.png",
                )
                self.logger.info("Generated memory metrics plot")

            # Plot GC time
            if "GC_Time_mean" in aggregated_results.columns:
                self.plotter.generate_plot(
                    {
                        "x": parallelism_levels,
                        "y": aggregated_results["GC_Time_mean"].tolist(),
                    },
                    plot_type="basic",
                    xlabel="Parallelism",
                    ylabel="Time (ms)",
                    title="GC Time Across Runs",
                    filename="gc_time.png",
                )
                self.logger.info("Generated GC time plot")

        except Exception as e:
            self.logger.error(f"Failed to generate resource metrics plots: {e}")

    def _plot_late_records(self, aggregated_results: pd.DataFrame) -> None:
        """Plot late records dropped metric."""
        if "LateRecordsDropped_total" not in aggregated_results.columns:
            return

        try:
            parallelism_levels = aggregated_results.index.tolist()
            late_records = aggregated_results["LateRecordsDropped_total"].tolist()

            self.plotter.generate_plot(
                {
                    "x": parallelism_levels,
                    "y": late_records,
                },
                plot_type="basic",
                xlabel="Parallelism",
                ylabel="Late Records Dropped (total)",
                title="Late Records Dropped Across Runs",
                filename="late_records_dropped.png",
            )
            self.logger.info("Generated late records dropped plot")
        except Exception as e:
            self.logger.error(f"Failed to generate late records plot: {e}")
