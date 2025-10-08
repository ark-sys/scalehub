import json
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd

from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)


class DefaultMultiRunProcessingStrategy(BaseProcessingStrategy):
    """
    Default strategy for processing multi-run experiments.

    This strategy:
    1. Delegates single run processing to SingleExperimentProcessor
       (loads from VictoriaMetrics, builds final_df.csv, generates plots)
    2. Aggregates results across runs
    3. Generates multi-run summary plots
    """

    def __init__(self, logger, exp_path: Path, config):
        super().__init__(logger, exp_path)
        self.config = config

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

        # Process each run using SingleExperimentProcessor
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

        # Aggregate results across runs
        aggregated_results = self._aggregate_runs(all_runs_data)

        # Export aggregated results as final_df.csv (multi-run level)
        final_df_path = self.exp_path / "final_df.csv"
        self.exporter.export_data(aggregated_results, final_df_path)
        self.logger.info(f"Multi-run final_df.csv saved to: {final_df_path}")

        # Generate summary plots across runs
        self._generate_summary_plots(aggregated_results)

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
        """
        Process a single run directory using SingleExperimentProcessor.

        The processor will:
        1. Check if final_df.csv exists, if not, build it from VictoriaMetrics
        2. Generate mean_stderr.csv with statistics
        3. Generate experiment and summary plots

        Returns the mean_stderr.csv data for aggregation.
        """
        self.logger.info(f"Processing run: {run_dir.name}")

        # Check if we need to build final_df.csv from VictoriaMetrics
        final_df_path = run_dir / "final_df.csv"
        if not final_df_path.exists():
            self.logger.info(
                f"final_df.csv not found, building from VictoriaMetrics..."
            )
            success = self._build_final_df_from_victoriametrics(run_dir)
            if not success:
                self.logger.warning(
                    f"Failed to build final_df.csv for run {run_dir.name}"
                )
                return None

        # Process the run using SingleExperimentProcessor
        try:
            from scripts.src.data.processing.single_experiment_processor import (
                SingleExperimentProcessor,
            )

            single_processor = SingleExperimentProcessor(
                self.logger, self.config, str(run_dir)
            )
            single_processor.process()
            self.logger.info(f"Successfully processed run {run_dir.name}")

            # Load the generated mean_stderr.csv for aggregation
            mean_stderr_path = run_dir / "mean_stderr.csv"
            if mean_stderr_path.exists():
                return pd.read_csv(mean_stderr_path, index_col=0)
            else:
                self.logger.warning(f"mean_stderr.csv not found for run {run_dir.name}")
                return None

        except Exception as e:
            self.logger.error(f"Failed to process run {run_dir.name}: {e}")
            return None

    def _build_final_df_from_victoriametrics(self, run_dir: Path) -> bool:
        """
        Build final_df.csv from VictoriaMetrics for a run that doesn't have it yet.

        This method:
        1. Loads exp_log.json to get timestamps
        2. Fetches metrics from VictoriaMetrics
        3. Exports raw metrics to export/ directory
        4. Builds final_df.csv using MetricsProcessor
        """
        from scripts.src.data.loading.loader import Loader
        from scripts.src.data.loading.strategies.victoria_metrics_load_strategy import (
            VictoriaMetricsLoadStrategy,
        )

        # Load exp_log.json to get timestamps
        exp_log_path = run_dir / "exp_log.json"
        if not exp_log_path.exists():
            self.logger.warning(f"No exp_log.json found in {run_dir.name}")
            return False

        with open(exp_log_path, "r") as f:
            exp_log = json.load(f)

        timestamps = exp_log.get("timestamps", {})
        start_ts = timestamps.get("start")
        end_ts = timestamps.get("end")

        if not start_ts or not end_ts:
            self.logger.warning(f"Missing timestamps in {run_dir.name}")
            return False

        # Get database URL from config
        db_url = self._get_db_url()

        # Load data from VictoriaMetrics
        # Note: VictoriaMetricsLoadStrategy returns Dict[str, Any] which can be
        # either pd.DataFrame (csv) or list (json) depending on format parameter
        vm_strategy = VictoriaMetricsLoadStrategy(
            self.logger, db_url, str(start_ts), str(end_ts)
        )
        vm_loader = Loader(vm_strategy)

        # Load both CSV (for export) and JSON (for processing)
        raw_data_csv = vm_loader.load_data(
            format="csv"
        )  # Returns Dict[str, pd.DataFrame]
        raw_data_json = vm_loader.load_data(format="json")  # Returns Dict[str, list]

        if not raw_data_json:
            self.logger.warning(
                f"No data loaded from VictoriaMetrics for run {run_dir.name}"
            )
            return False

        # Export raw metrics to CSV and JSON files
        self._export_raw_metrics(raw_data_csv, raw_data_json, run_dir)

        # Build final_df.csv with multi-column structure
        return self._build_final_dataframe(raw_data_json, run_dir, exp_log)

    def _export_raw_metrics(
        self,
        raw_data_csv: Dict[str, Any],
        raw_data_json: Dict[str, Any],
        run_dir: Path,
    ) -> None:
        """Export raw metrics to CSV and JSON files for archival."""
        export_dir = run_dir / "export"
        export_dir.mkdir(exist_ok=True)

        # Export CSV files (Dict[str, pd.DataFrame])
        if raw_data_csv:
            for metric_name, df in raw_data_csv.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    output_path = export_dir / f"{metric_name}_export.csv"
                    self.exporter.export_data(df, output_path)

        # Export JSON files (Dict[str, list])
        if raw_data_json:
            for metric_name, json_data in raw_data_json.items():
                if isinstance(json_data, list) and json_data:
                    output_path = export_dir / f"{metric_name}_export.json"
                    with open(output_path, "w") as f:
                        for item in json_data:
                            f.write(json.dumps(item) + "\n")

    def _build_final_dataframe(
        self, raw_data_json: Dict[str, Any], run_dir: Path, exp_log: dict
    ) -> bool:
        """Build final_df.csv with multi-column structure from JSON data."""
        if not raw_data_json:
            self.logger.warning("No JSON data available to build final_df")
            return False

        try:
            from scripts.src.data.processing.metrics_processor import MetricsProcessor

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
                self.exporter.export_data(final_df, output_path)
                self.logger.info(f"Built final_df.csv with shape {final_df.shape}")
                return True
            else:
                self.logger.warning("Final DataFrame is empty, skipping export")
                return False

        except Exception as e:
            self.logger.error(f"Failed to build final_df: {e}")
            return False

    def _get_db_url(self) -> str:
        """Get VictoriaMetrics URL from config or use default."""
        try:
            db_url = self.config.get_str(
                "experiment.db_url",
                "victoria-metrics.monitoring.svc.cluster.local:8428",
            )
        except (KeyError, AttributeError, ValueError):
            db_url = "victoria-metrics.monitoring.svc.cluster.local:8428"
        return db_url

    def _aggregate_runs(self, all_runs_data: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Aggregate statistics across all runs.

        For each parallelism level, calculates:
        - Mean, std, min, max of throughput
        - Mean of other metrics
        """
        if not all_runs_data:
            return pd.DataFrame()

        self.logger.info("Aggregating results across runs...")

        # Combine all mean_stderr.csv data with run identifiers
        combined_df = pd.concat(all_runs_data, keys=range(1, len(all_runs_data) + 1))
        combined_df.index.names = ["Run", "Parallelism"]

        # Calculate statistics across runs for each parallelism level
        agg_dict = {
            "Throughput": ["mean", "std", "min", "max"],
            "ThroughputStdErr": "mean",
        }

        # Add other metrics if they exist
        if "BusyTime" in combined_df.columns:
            agg_dict["BusyTime"] = ["mean", "std"]
            agg_dict["BusyTimeStdErr"] = "mean"

        if "BackpressureTime" in combined_df.columns:
            agg_dict["BackpressureTime"] = ["mean", "std"]
            agg_dict["BackpressureTimeStdErr"] = "mean"

        final_df = combined_df.groupby("Parallelism").agg(agg_dict)

        # Flatten multi-level columns
        final_df.columns = ["_".join(col).strip("_") for col in final_df.columns]

        # Rename columns to match expected format
        rename_map = {
            "Throughput_mean": "Throughput",
            "ThroughputStdErr_mean": "ThroughputStdErr",
        }

        if "BusyTime_mean" in final_df.columns:
            rename_map.update(
                {
                    "BusyTime_mean": "BusyTime",
                    "BusyTimeStdErr_mean": "BusyTimeStdErr",
                }
            )

        if "BackpressureTime_mean" in final_df.columns:
            rename_map.update(
                {
                    "BackpressureTime_mean": "BackpressureTime",
                    "BackpressureTimeStdErr_mean": "BackpressureTimeStdErr",
                }
            )

        final_df = final_df.rename(columns=rename_map)

        return final_df

    def _generate_summary_plots(self, aggregated_results: pd.DataFrame) -> None:
        """Generate summary plots for the multi-run experiment."""
        self.logger.info("Generating multi-run summary plots...")

        if aggregated_results.empty:
            self.logger.warning("No aggregated results to plot")
            return

        # Plot throughput with min/max range across runs
        self._plot_throughput(aggregated_results)

        # Plot busy time and backpressure time (if available)
        self._plot_time_metrics(aggregated_results)

        self.logger.info("Summary plots generated successfully")

    def _plot_throughput(self, aggregated_results: pd.DataFrame) -> None:
        """Plot throughput with min/max error bars."""
        if "Throughput" not in aggregated_results.columns:
            return

        try:
            parallelism_levels = aggregated_results.index.tolist()
            throughput_mean = aggregated_results["Throughput"].tolist()
            throughput_min = aggregated_results.get(
                "Throughput_min", throughput_mean
            ).tolist()
            throughput_max = aggregated_results.get(
                "Throughput_max", throughput_mean
            ).tolist()

            # Create error bars from min/max
            yerr_lower = [
                mean - min_val for mean, min_val in zip(throughput_mean, throughput_min)
            ]
            yerr_upper = [
                max_val - mean for mean, max_val in zip(throughput_mean, throughput_max)
            ]

            # Calculate ylim: 0 to max_value + 10%
            max_value = max(throughput_max)
            ylim_max = max_value * 1.1

            self.plotter.generate_plot(
                {
                    "x": parallelism_levels,
                    "y": throughput_mean,
                    "yerr": [yerr_lower, yerr_upper],
                },
                plot_type="basic",
                xlabel="Parallelism",
                ylabel="Throughput (records/s)",
                ylim=(0, ylim_max),
                filename="throughput_across_runs.png",
            )
            self.logger.info("Generated throughput plot")
        except Exception as e:
            self.logger.error(f"Failed to generate throughput plot: {e}")

    def _plot_time_metrics(self, aggregated_results: pd.DataFrame) -> None:
        """Plot busy time and backpressure time with fixed y-scale."""
        if (
            "BusyTime" not in aggregated_results.columns
            or "BackpressureTime" not in aggregated_results.columns
        ):
            return

        try:
            parallelism_levels = aggregated_results.index.tolist()

            # Create DataFrame for stacked plot
            plot_df = pd.DataFrame(
                {
                    "BusyTime": aggregated_results["BusyTime"],
                    "BackpressureTime": aggregated_results["BackpressureTime"],
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
                filename="time_metrics_across_runs.png",
            )
            self.logger.info("Generated time metrics plot")
        except Exception as e:
            self.logger.error(f"Failed to generate time metrics plot: {e}")
