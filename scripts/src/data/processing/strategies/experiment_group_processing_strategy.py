"""
Processing strategy for experiment groups with multiple configurations.

Handles processing of experiment groups (e.g., Experiment 1, 2, 3) where each group
contains multiple configurations (e.g., a1, a2, a3) and each configuration has multiple runs.
Generates comparative plots across configurations with error bars from multiple runs.
"""
import json
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import numpy as np

from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)


class ExperimentGroupProcessingStrategy(BaseProcessingStrategy):
    """
    Strategy for processing experiment groups with multiple configurations.
    
    This strategy:
    1. Identifies all experiment configurations in a directory (a1, a2, a3, etc.)
    2. For each configuration, aggregates data across multiple runs
    3. Generates comparative plots showing differences between configurations
    4. Includes error bars from run variance
    """

    # Experiment group definitions
    EXPERIMENT_GROUPS = {
        'a': {
            'name': 'Experiment 1 (1 core per task slot)',
            'configs': ['a1', 'a2', 'a3'],
            'labels': ['(i) 1 TM,\n52 TS/TM', '(ii) 26 TM,\n2 TS/TM', '(iii) 52 TM,\n1 TS/TM']
        },
        'b': {
            'name': 'Experiment 2 (2 cores per task slot)',
            'configs': ['b1', 'b2', 'b3'],
            'labels': ['(i) 1 TM,\n26 TS/TM', '(ii) 13 TM,\n2 TS/TM', '(iii) 26 TM,\n1 TS/TM']
        },
        'c': {
            'name': 'Experiment 3 (4 cores per task slot)',
            'configs': ['c1', 'c2'],
            'labels': ['(i) 1 TM,\n13 TS/TM', '(ii) 13 TM,\n1 TS/TM']
        }
    }

    def __init__(self, logger, exp_path: Path):
        super().__init__(logger, exp_path)
        self.experiment_groups = self._detect_all_experiment_groups()

    def process(self) -> Dict[str, Any]:
        """Main processing workflow for all experiment groups."""
        if not self.experiment_groups:
            self.logger.error("Could not detect any experiment groups")
            return {"type": "experiment_group", "status": "error", "message": "No experiment groups found"}

        self.logger.info(f"Found {len(self.experiment_groups)} experiment groups to process: {self.experiment_groups}")

        results = {}
        for group_key in self.experiment_groups:
            self.logger.info(f"Processing experiment group: {group_key}")
            group_result = self._process_single_group(group_key)
            results[group_key] = group_result

        return {
            "type": "experiment_group",
            "status": "success",
            "groups_processed": list(results.keys()),
            "details": results
        }

    def _detect_all_experiment_groups(self) -> List[str]:
        """Detect all experiment groups present in this directory."""
        subdirs = [d.name for d in self.exp_path.iterdir() if d.is_dir()]
        found_groups = []

        for group_key, group_info in self.EXPERIMENT_GROUPS.items():
            configs = group_info['configs']
            # If we find any of the expected configs for this group, include it
            if any(config in subdirs for config in configs):
                found_groups.append(group_key)

        return sorted(found_groups)

    def _process_single_group(self, group_key: str) -> Dict[str, Any]:
        """Process a single experiment group."""
        # Find all configuration directories for this group
        config_dirs = self._get_configuration_directories(group_key)
        if not config_dirs:
            self.logger.warning(f"No configuration directories found for group {group_key}")
            return {"status": "error", "message": "No configurations found"}

        self.logger.info(f"Found {len(config_dirs)} configurations for group {group_key}")

        # Load aggregated results from each configuration
        config_results = self._load_configuration_results(config_dirs)
        
        if not config_results:
            self.logger.warning(f"No configuration results loaded for group {group_key}")
            return {"status": "error", "message": "No data loaded"}

        # Generate comparison plots for this group
        self._generate_comparison_plots(group_key, config_results)

        # Generate job-level metrics plot
        self._generate_job_level_plots(group_key, config_results)

        # Generate resource metrics plots
        self._generate_resource_plots(group_key, config_results)

        return {
            "status": "success",
            "configs_processed": len(config_results)
        }

    def _get_configuration_directories(self, group_key: str) -> List[Path]:
        """Get all configuration directories for a specific experiment group."""
        expected_configs = self.EXPERIMENT_GROUPS[group_key]['configs']
        config_dirs = []
        
        for config_name in expected_configs:
            config_path = self.exp_path / config_name
            if config_path.exists() and config_path.is_dir():
                config_dirs.append(config_path)
            else:
                self.logger.warning(f"Configuration directory not found: {config_name}")
        
        return config_dirs

    def _load_configuration_results(self, config_dirs: List[Path]) -> Dict[str, pd.DataFrame]:
        """Load aggregated results from each configuration directory."""
        results = {}
        
        for config_dir in config_dirs:
            config_name = config_dir.name
            
            # Look for aggregated_results.csv or final_df.csv
            result_files = ['aggregated_results.csv', 'final_df.csv']
            
            for result_file in result_files:
                result_path = config_dir / result_file
                if result_path.exists():
                    try:
                        df = pd.read_csv(result_path, index_col=0)
                        results[config_name] = df
                        self.logger.info(f"Loaded {result_file} for {config_name}: shape {df.shape}")
                        self.logger.debug(f"Columns: {df.columns.tolist()}")
                        break
                    except Exception as e:
                        self.logger.error(f"Error loading {result_path}: {e}")
        
        return results

    def _generate_comparison_plots(self, group_key: str, config_results: Dict[str, pd.DataFrame]) -> None:
        """Generate comparison plots across configurations using scientific style."""
        self.logger.info(f"Generating comparison plots for group {group_key}...")

        group_info = self.EXPERIMENT_GROUPS[group_key]
        expected_configs = group_info['configs']
        labels = group_info['labels']
        
        # Prepare data for plotting
        throughput_means = []
        throughput_stds = []
        busytime_means = []
        busytime_stds = []
        backpressure_means = []
        backpressure_stds = []
        valid_labels = []

        for i, config in enumerate(expected_configs):
            if config in config_results:
                df = config_results[config]

                # Extract throughput data (handle both single and multi-parallelism cases)
                if 'Throughput' in df.columns:
                    # Take the mean across all parallelism levels if multiple rows
                    throughput_means.append(df['Throughput'].mean())

                    # Use std if available, otherwise stderr
                    if 'Throughput_std' in df.columns:
                        throughput_stds.append(df['Throughput_std'].mean())
                    elif 'ThroughputStdErr' in df.columns:
                        throughput_stds.append(df['ThroughputStdErr'].mean())
                    else:
                        throughput_stds.append(0)
                else:
                    throughput_means.append(0)
                    throughput_stds.append(0)

                # Extract busy time data
                if 'BusyTime' in df.columns:
                    busytime_means.append(df['BusyTime'].mean())
                    if 'BusyTime_std' in df.columns:
                        busytime_stds.append(df['BusyTime_std'].mean())
                    elif 'BusyTimeStdErr' in df.columns:
                        busytime_stds.append(df['BusyTimeStdErr'].mean())
                    else:
                        busytime_stds.append(0)
                else:
                    busytime_means.append(0)
                    busytime_stds.append(0)

                # Extract backpressure data
                if 'BackpressureTime' in df.columns:
                    backpressure_means.append(df['BackpressureTime'].mean())
                    if 'BackpressureTime_std' in df.columns:
                        backpressure_stds.append(df['BackpressureTime_std'].mean())
                    elif 'BackpressureTimeStdErr' in df.columns:
                        backpressure_stds.append(df['BackpressureTimeStdErr'].mean())
                    else:
                        backpressure_stds.append(0)
                else:
                    backpressure_means.append(0)
                    backpressure_stds.append(0)

                valid_labels.append(labels[i])

        if not valid_labels:
            self.logger.warning("No valid data for comparison plots")
            return

        # Generate throughput plot with scientific style
        self._plot_throughput_scientific(
            group_key, valid_labels, throughput_means, throughput_stds, group_info['name']
        )

        # Generate time metrics plot if we have the data
        if any(busytime_means) or any(backpressure_means):
            self._plot_time_metrics_scientific(
                group_key, valid_labels, busytime_means, busytime_stds,
                backpressure_means, backpressure_stds, group_info['name']
            )

    def _plot_throughput_scientific(self, group_key: str, labels: List[str], means: List[float],
                                     stds: List[float], title: str) -> None:
        """Plot throughput comparison with scientific style and error bars."""
        try:
            plot_data = {
                'x': list(range(len(labels))),
                'y': means,
                'yerr': stds,
                'labels': labels
            }
            
            self.plotter.generate_plot(
                plot_data,
                plot_type="scientific",
                style="scatter",  # Use scatter instead of line for categorical x-axis
                xlabel="TaskManager Configuration",
                ylabel="Throughput (records/s)",
                title=None,  # No title - will be provided externally
                ylim=(0, max(means) * 1.2 if max(means) > 0 else 1),
                filename=f"{group_key}_throughput_comparison.png"
            )
            
            self.logger.info(f"Generated throughput comparison plot for group {group_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate throughput comparison plot for group {group_key}: {e}")

    def _plot_time_metrics_scientific(self, group_key: str, labels: List[str],
                                      busy_means: List[float], busy_stds: List[float],
                                      bp_means: List[float], bp_stds: List[float],
                                      title: str) -> None:
        """Plot time metrics with dual axis and scientific style."""
        try:
            plot_data = {
                'x': list(range(len(labels))),
                'y1': busy_means,
                'y2': bp_means,
                'y1_err': busy_stds,
                'y2_err': bp_stds,
                'labels': labels
            }

            self.plotter.generate_plot(
                plot_data,
                plot_type="scientific",
                style="dual_axis",
                xlabel="TaskManager Configuration",
                y1_label="Busy Time (ms/s)",
                y2_label="Backpressure Time (ms/s)",
                title=None,  # No title - will be provided externally
                filename=f"{group_key}_time_metrics_comparison.png"
            )
            
            self.logger.info(f"Generated time metrics plot for group {group_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate time metrics plot for group {group_key}: {e}")

    def _generate_job_level_plots(self, group_key: str, config_results: Dict[str, pd.DataFrame]) -> None:
        """Generate job-level metrics plots (checkpointing, etc.)."""
        # Job-level plots are now handled in _plot_checkpoint_metrics
        pass

    def _generate_resource_plots(self, group_key: str, config_results: Dict[str, pd.DataFrame]) -> None:
        """Generate resource utilization plots."""
        self.logger.info(f"Generating resource metrics plots for group {group_key}...")

        # Load resource metrics from individual runs for each configuration
        resource_data = self._load_resource_metrics(group_key)

        if not resource_data:
            self.logger.warning(f"No resource metrics data available for group {group_key}")
            return

        # Generate CPU metrics plot
        self._plot_cpu_metrics(group_key, resource_data)

        # Generate memory metrics plot
        self._plot_memory_metrics(group_key, resource_data)

        # Generate GC metrics plot
        self._plot_gc_metrics(group_key, resource_data)

        # Generate checkpoint metrics plot
        self._plot_checkpoint_metrics(group_key, resource_data)

    def _load_resource_metrics(self, group_key: str) -> Dict[str, Dict[str, Any]]:
        """
        Load resource metrics from individual runs and TaskManagers for each configuration.
        Loads from JSON files to get accurate per-TaskManager data.

        Returns: Dict[config_name, Dict[metric_name, data]]
        For most metrics: {'mean': float, 'std': float, 'values': list}
        For GC metrics: {'tm_data': List[Dict], 'mean': float, 'std': float}
            where tm_data contains per-TaskManager stats across runs
        """
        group_info = self.EXPERIMENT_GROUPS[group_key]
        expected_configs = group_info['configs']
        resource_data = {}

        for config in expected_configs:
            config_path = self.exp_path / config
            if not config_path.exists():
                continue

            # Find all run directories (numbered subdirectories)
            run_dirs = sorted([d for d in config_path.iterdir() if d.is_dir() and d.name.isdigit()])

            if not run_dirs:
                self.logger.warning(f"No run directories found for config {config}")
                continue

            # Collect metrics from all runs
            run_metrics = {
                'cpu_load': [],
                'cpu_time': [],
                'threads': [],
                'heap_memory': [],
                'managed_memory': [],
                'gc_time': [],
                'checkpoint_duration': [],
                'checkpoint_alignment': []
            }

            # For per-TaskManager GC data (to plot individual TMs)
            gc_tm_runs = []  # List of dicts: [{tm_id: value, ...}, ...]

            for run_dir in run_dirs:
                export_dir = run_dir / 'export'
                if not export_dir.exists():
                    continue

                # Mapping of metric keys to JSON filenames
                metrics_to_load = {
                    'cpu_load': 'flink_taskmanager_Status_JVM_CPU_Load_export.json',
                    'cpu_time': 'flink_taskmanager_Status_JVM_CPU_Time_export.json',
                    'threads': 'flink_taskmanager_Status_JVM_Threads_Count_export.json',
                    'heap_memory': 'flink_taskmanager_Status_JVM_Memory_Heap_Used_export.json',
                    'managed_memory': 'flink_taskmanager_Status_Flink_Memory_Managed_Used_export.json',
                }

                # Metrics that should be averaged (not summed) across TaskManagers
                # CPU load is now summed to show total CPU utilization across all TMs on the same machine
                metrics_to_average = set()

                # Load metrics that should be aggregated across TaskManagers
                for metric_key, filename in metrics_to_load.items():
                    file_path = export_dir / filename
                    if file_path.exists():
                        try:
                            # Load JSON file (contains one JSON object per TaskManager per line)
                            all_tm_values = []  # List of value arrays, one per TaskManager
                            line_count = 0
                            with open(file_path, 'r') as f:
                                for line in f:
                                    if line.strip():
                                        line_count += 1
                                        data = json.loads(line)
                                        values = data.get('values', [])
                                        if values:
                                            # Filter out None/null values from the time series
                                            filtered_values = [v for v in values if v is not None]
                                            if filtered_values:
                                                all_tm_values.append(filtered_values)
                                                # Log details for first few lines if threads
                                                if metric_key == 'threads' and line_count <= 3:
                                                    tm_id = data.get('metric', {}).get('tm_id', 'unknown')
                                                    self.logger.info(f"[{config}/{run_dir.name}] Thread line {line_count}: TM_ID={tm_id}, values_count={len(filtered_values)}, mean={np.mean(filtered_values):.2f}")

                            if all_tm_values:
                                num_tms = len(all_tm_values)
                                if metric_key in metrics_to_average:
                                    # For metrics that should be averaged (empty set now)
                                    tm_means = [np.mean(values) for values in all_tm_values]
                                    aggregated_value = np.mean(tm_means)
                                else:
                                    # For threads, memory, cpu_time, cpu_load: sum across TMs at each time point, then take mean
                                    # Find the minimum length to align time series
                                    min_length = min(len(values) for values in all_tm_values)
                                    # Truncate all series to same length
                                    aligned_values = [values[:min_length] for values in all_tm_values]
                                    # Sum across TaskManagers at each time point
                                    summed_series = np.sum(aligned_values, axis=0)
                                    # Take mean of the summed time series
                                    aggregated_value = np.mean(summed_series)

                                # Log thread count and CPU load details for debugging
                                if metric_key == 'threads':
                                    self.logger.info(f"[{config}/{run_dir.name}] Threads: {num_tms} TMs detected (from {line_count} lines), aggregated value: {aggregated_value:.2f}")

                                if metric_key == 'cpu_load':
                                    # Log first 3 TMs for debugging
                                    for idx in range(min(3, len(all_tm_values))):
                                        tm_mean = np.mean(all_tm_values[idx])
                                        self.logger.info(f"[{config}/{run_dir.name}] CPU Load TM {idx+1}: mean={tm_mean:.4f}")
                                    self.logger.info(f"[{config}/{run_dir.name}] CPU Load: {num_tms} TMs detected, aggregated value: {aggregated_value:.4f}")

                                run_metrics[metric_key].append(aggregated_value)
                            else:
                                if metric_key == 'threads':
                                    self.logger.warning(f"[{config}/{run_dir.name}] No thread data found in {filename} (file had {line_count} lines)")

                        except Exception as e:
                            self.logger.error(f"Error loading {filename} from {run_dir.name}: {e}")
                            import traceback
                            self.logger.debug(traceback.format_exc())
                # Load GC time with per-TaskManager detail
                gc_file = export_dir / 'flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time_export.json'
                if gc_file.exists():
                    try:
                        tm_gc_data = {}
                        with open(gc_file, 'r') as f:
                            for line in f:
                                if line.strip():
                                    data = json.loads(line)
                                    tm_id = data.get('metric', {}).get('tm_id', 'unknown')
                                    values = data.get('values', [])
                                    if values:
                                        # Filter out None/null values
                                        filtered_values = [v for v in values if v is not None]
                                        if filtered_values:
                                            # Store mean for this TaskManager in this run
                                            # Use tm_id as key to identify the same TM across runs
                                            tm_gc_data[tm_id] = np.mean(filtered_values)

                        if tm_gc_data:
                            gc_tm_runs.append(tm_gc_data)
                            # Also compute aggregated value
                            aggregated_gc = np.sum(list(tm_gc_data.values()))
                            run_metrics['gc_time'].append(aggregated_gc)
                            self.logger.info(f"[{config}/{run_dir.name}] GC: Loaded {len(tm_gc_data)} TaskManagers")
                        else:
                            self.logger.warning(f"[{config}/{run_dir.name}] GC file exists but no valid data found")

                    except Exception as e:
                        self.logger.error(f"Error loading GC metrics from {run_dir.name}: {e}")
                        import traceback
                        self.logger.debug(traceback.format_exc())
                else:
                    self.logger.warning(f"[{config}/{run_dir.name}] GC file not found: {gc_file.name}")

                # Load job-level checkpoint metrics (single value, use CSV)
                for metric_key, filename in [
                    ('checkpoint_duration', 'flink_jobmanager_job_lastCheckpointDuration_export.csv'),
                    ('checkpoint_alignment', 'flink_taskmanager_job_task_checkpointStartDelayNanos_export.csv')
                ]:
                    file_path = export_dir / filename
                    if file_path.exists():
                        try:
                            df = pd.read_csv(file_path)
                            if 'Value' in df.columns and len(df) > 0:
                                mean_value = df['Value'].mean()
                                run_metrics[metric_key].append(mean_value)
                        except Exception as e:
                            self.logger.debug(f"Error loading {filename} from {run_dir.name}: {e}")

            # Aggregate across runs
            config_metrics = {}
            for metric_key, values in run_metrics.items():
                if values:
                    config_metrics[metric_key] = {
                        'mean': np.mean(values),
                        'std': np.std(values),
                        'values': values
                    }

            # Process per-TaskManager GC data
            # The issue was here - we need to aggregate by TaskManager position/index, not by tm_id
            # because tm_ids are unique per run, not consistent across runs
            if gc_tm_runs:
                self.logger.info(f"Processing GC data for config {config}: {len(gc_tm_runs)} runs")

                # For each run, we have a dict of {tm_id: gc_value}
                # Since tm_ids are unique per run (e.g., "10_42_1_19:6122_263da0"),
                # we can't match by ID. Instead, we should match by TaskManager position/index.

                # Get the number of TaskManagers from the first run
                num_tms_per_run = [len(run_data) for run_data in gc_tm_runs]
                if len(set(num_tms_per_run)) > 1:
                    self.logger.warning(f"[{config}] Inconsistent TM counts across runs: {num_tms_per_run}")

                # Use the most common number of TaskManagers
                from collections import Counter
                num_tms = Counter(num_tms_per_run).most_common(1)[0][0]
                self.logger.info(f"[{config}] Number of TaskManagers per run: {num_tms}")

                # Aggregate by TaskManager index (position in sorted list)
                tm_stats = []
                for tm_idx in range(num_tms):
                    # Collect GC values for this TaskManager index across all runs
                    tm_values = []
                    for run_data in gc_tm_runs:
                        # Sort tm_ids to ensure consistent ordering
                        sorted_tms = sorted(run_data.keys())
                        if tm_idx < len(sorted_tms):
                            tm_id = sorted_tms[tm_idx]
                            tm_values.append(run_data[tm_id])

                    if tm_values:
                        tm_stats.append({
                            'tm_id': f'TM_{tm_idx}',  # Generic ID since actual IDs differ per run
                            'mean': np.mean(tm_values),
                            'std': np.std(tm_values),
                            'values': tm_values
                        })
                        self.logger.debug(f"[{config}] TM {tm_idx}: {len(tm_values)} runs, mean={np.mean(tm_values):.2f}, std={np.std(tm_values):.2f}")

                self.logger.info(f"[{config}] Aggregated GC data for {len(tm_stats)} TaskManagers")

                if 'gc_time' in config_metrics:
                    config_metrics['gc_time']['tm_data'] = tm_stats
                else:
                    config_metrics['gc_time'] = {
                        'mean': 0,
                        'std': 0,
                        'tm_data': tm_stats
                    }

            if config_metrics:
                resource_data[config] = config_metrics

        return resource_data

    def _plot_cpu_metrics(self, group_key: str, resource_data: Dict[str, Dict[str, Any]]) -> None:
        """Plot CPU-related metrics (CPU Load, CPU Time, Thread Count)."""
        try:
            group_info = self.EXPERIMENT_GROUPS[group_key]
            expected_configs = group_info['configs']
            labels = group_info['labels']

            # Prepare data
            cpu_load_means = []
            cpu_load_stds = []
            cpu_time_means = []
            cpu_time_stds = []
            thread_means = []
            thread_stds = []
            valid_labels = []

            for i, config in enumerate(expected_configs):
                if config in resource_data:
                    metrics = resource_data[config]

                    if 'cpu_load' in metrics:
                        cpu_load_means.append(metrics['cpu_load']['mean'])
                        cpu_load_stds.append(metrics['cpu_load']['std'])
                        self.logger.debug(f"Config {config} CPU Load: mean={metrics['cpu_load']['mean']}, std={metrics['cpu_load']['std']}")
                    else:
                        cpu_load_means.append(0)
                        cpu_load_stds.append(0)

                    if 'cpu_time' in metrics:
                        # Convert to seconds if needed
                        cpu_time_means.append(metrics['cpu_time']['mean'] / 1e9)  # nanoseconds to seconds
                        cpu_time_stds.append(metrics['cpu_time']['std'] / 1e9)
                    else:
                        cpu_time_means.append(0)
                        cpu_time_stds.append(0)

                    if 'threads' in metrics:
                        thread_means.append(metrics['threads']['mean'])
                        thread_stds.append(metrics['threads']['std'])
                        self.logger.debug(f"Config {config} Threads: mean={metrics['threads']['mean']}, std={metrics['threads']['std']}")
                    else:
                        thread_means.append(0)
                        thread_stds.append(0)

                    valid_labels.append(labels[i])

            if not valid_labels:
                return

            # Generate dual-axis plot: CPU Load (left) vs Thread Count (right)
            if any(cpu_load_means) and any(thread_means):
                plot_data = {
                    'x': list(range(len(valid_labels))),
                    'y1': cpu_load_means,
                    'y2': thread_means,
                    'y1_err': cpu_load_stds,
                    'y2_err': thread_stds,
                    'labels': valid_labels
                }

                self.plotter.generate_plot(
                    plot_data,
                    plot_type="scientific",
                    style="dual_axis",
                    use_lines=False,  # No lines for categorical x-axis
                    xlabel="TaskManager Configuration",
                    y1_label="CPU Load",
                    y2_label="Thread Count",
                    title=None,  # No title - will be provided externally
                    legend_loc='upper left',  # Place legend to avoid overlap
                    markersize=18,  # Smaller markers to show error bars better
                    y1_lim=(0, 26),  # Assuming max CPU load is 26 (for 26 cores)
                    # y1_lim=(0, max(cpu_load_means) * 1.2 if max(cpu_load_means) > 0 else 1),
                    y2_lim=(0, 4000),
                    # y2_lim=(0, max(thread_means) * 1.2 if max(thread_means) > 0 else 1),
                    filename=f"{group_key}_cpu_metrics.png"
                )

                self.logger.info(f"Generated CPU metrics plot for group {group_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate CPU metrics plot for group {group_key}: {e}")

    def _plot_memory_metrics(self, group_key: str, resource_data: Dict[str, Dict[str, Any]]) -> None:
        """Plot memory metrics (Heap Memory, Managed Memory)."""
        try:
            group_info = self.EXPERIMENT_GROUPS[group_key]
            expected_configs = group_info['configs']
            labels = group_info['labels']

            # Prepare data
            heap_means = []
            heap_stds = []
            managed_means = []
            managed_stds = []
            valid_labels = []

            for i, config in enumerate(expected_configs):
                if config in resource_data:
                    metrics = resource_data[config]

                    if 'heap_memory' in metrics:
                        # Convert to GB
                        heap_means.append(metrics['heap_memory']['mean'] / (1024**3))
                        heap_stds.append(metrics['heap_memory']['std'] / (1024**3))
                    else:
                        heap_means.append(0)
                        heap_stds.append(0)

                    if 'managed_memory' in metrics:
                        # Convert to GB
                        managed_means.append(metrics['managed_memory']['mean'] / (1024**3))
                        managed_stds.append(metrics['managed_memory']['std'] / (1024**3))
                    else:
                        managed_means.append(0)
                        managed_stds.append(0)

                    valid_labels.append(labels[i])

            if not valid_labels:
                return

            # Generate dual-axis plot: Heap Memory (left) vs Managed Memory (right)
            if any(heap_means) or any(managed_means):
                plot_data = {
                    'x': list(range(len(valid_labels))),
                    'y1': heap_means,
                    'y2': managed_means,
                    'y1_err': heap_stds,
                    'y2_err': managed_stds,
                    'labels': valid_labels
                }

                self.plotter.generate_plot(
                    plot_data,
                    plot_type="scientific",
                    style="dual_axis",
                    use_lines=False,  # No lines for categorical x-axis
                    xlabel="TaskManager Configuration",
                    y1_label="Heap Memory (GB)",
                    y2_label="Managed Memory (GB)",
                    title=None,  # No title - will be provided externally
                    markersize=18,  # Consistent marker size across all plots
                    y1_lim=(0, max(heap_means) * 1.2 if max(heap_means) > 0 else 1),
                    y2_lim=(0, max(managed_means) * 1.2 if max(managed_means) > 0 else 1),
                    filename=f"{group_key}_memory_metrics.png"
                )

                self.logger.info(f"Generated memory metrics plot for group {group_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate memory metrics plot for group {group_key}: {e}")

    def _plot_gc_metrics(self, group_key: str, resource_data: Dict[str, Dict[str, Any]]) -> None:
        """
        Plot combined GC and checkpoint metrics in a single plot.
        All metrics are displayed in seconds on a unified y-axis.
        - GC Time: triangle markers (one per TaskManager with jitter)
        - Checkpoint Duration: circle markers
        - Checkpoint Alignment: square markers
        Colors differentiate metric types, not configurations.
        """
        try:
            import matplotlib.pyplot as plt
            from matplotlib.ticker import FuncFormatter

            group_info = self.EXPERIMENT_GROUPS[group_key]
            expected_configs = group_info['configs']
            labels = group_info['labels']

            # Log what data is available
            self.logger.info(f"Starting combined GC + Checkpoint plot for group {group_key}")
            for config in expected_configs:
                if config in resource_data:
                    metrics = resource_data[config]
                    if 'gc_time' in metrics and 'tm_data' in metrics['gc_time']:
                        num_tms = len(metrics['gc_time']['tm_data'])
                        self.logger.info(f"  Config {config}: {num_tms} TaskManagers with GC data")
                    if 'checkpoint_duration' in metrics:
                        self.logger.info(f"  Config {config}: Checkpoint duration available")
                    if 'checkpoint_alignment' in metrics:
                        self.logger.info(f"  Config {config}: Checkpoint alignment available")

            # Setup scientific style
            figsize = (14, 10)
            fontsize = 32
            tick_size = 32
            markersize = 18
            markeredgewidth = 2.0
            capsize = 8
            grid_alpha = 0.3
            dpi = 600

            # Colors for different METRICS (not configurations)
            gc_color = "#1f77b4"  # Blue for GC Time
            duration_color = "#ff7f0e"  # Orange for Checkpoint Duration
            alignment_color = "#2ca02c"  # Green for Checkpoint Alignment

            plt.rcParams.update({
                'font.family': 'serif',
                'font.size': fontsize,
                'axes.labelsize': fontsize,
                'xtick.labelsize': tick_size,
                'ytick.labelsize': tick_size,
                'grid.alpha': grid_alpha
            })

            fig, ax = plt.subplots(figsize=figsize)

            all_values = []
            num_plotted = 0

            # Plot per-TaskManager GC data (triangles with jitter)
            for config_idx, config in enumerate(expected_configs):
                if config not in resource_data:
                    continue

                metrics = resource_data[config]

                # Plot GC data (triangles in blue)
                if 'gc_time' in metrics and 'tm_data' in metrics['gc_time']:
                    tm_data = metrics['gc_time']['tm_data']
                    num_tms = len(tm_data)
                    jitter_width = min(0.15, 0.3 / max(num_tms, 1))

                    for tm_idx, tm_stat in enumerate(tm_data):
                        mean_gc = tm_stat['mean'] / 1000.0  # ms to seconds
                        std_gc = tm_stat['std'] / 1000.0
                        all_values.append(mean_gc)

                        if num_tms > 1:
                            jitter = (tm_idx / (num_tms - 1) - 0.5) * 2 * jitter_width
                        else:
                            jitter = 0

                        x_pos = config_idx + jitter

                        ax.errorbar(
                            x_pos, mean_gc,
                            yerr=std_gc,
                            fmt='^',  # Triangle marker for GC
                            markersize=markersize,
                            markeredgecolor='black',
                            markeredgewidth=markeredgewidth,
                            color=gc_color,  # Blue for all GC points
                            capsize=capsize,
                            capthick=2,
                            elinewidth=2,
                            zorder=3,
                            alpha=0.7,
                            label='GC Time' if config_idx == 0 and tm_idx == 0 else None
                        )
                        num_plotted += 1

                # Plot Checkpoint Duration (circles in orange)
                if 'checkpoint_duration' in metrics:
                    duration_mean = metrics['checkpoint_duration']['mean'] / 1000.0  # ms to seconds
                    duration_std = metrics['checkpoint_duration']['std'] / 1000.0
                    all_values.append(duration_mean)

                    ax.errorbar(
                        config_idx, duration_mean,
                        yerr=duration_std,
                        fmt='o',  # Circle marker for checkpoint duration
                        markersize=markersize,
                        markeredgecolor='black',
                        markeredgewidth=markeredgewidth,
                        color=duration_color,  # Orange for all checkpoint duration points
                        capsize=capsize,
                        capthick=2,
                        elinewidth=2,
                        zorder=4,
                        label='Checkpoint Duration' if config_idx == 0 else None
                    )
                    num_plotted += 1

                # Plot Checkpoint Alignment (squares in green)
                if 'checkpoint_alignment' in metrics:
                    alignment_mean = metrics['checkpoint_alignment']['mean'] / 1e9  # ns to seconds
                    alignment_std = metrics['checkpoint_alignment']['std'] / 1e9
                    all_values.append(alignment_mean)

                    ax.errorbar(
                        config_idx, alignment_mean,
                        yerr=alignment_std,
                        fmt='s',  # Square marker for checkpoint alignment
                        markersize=markersize,
                        markeredgecolor='black',
                        markeredgewidth=markeredgewidth,
                        color=alignment_color,  # Green for all checkpoint alignment points
                        capsize=capsize,
                        capthick=2,
                        elinewidth=2,
                        zorder=4,
                        label='Checkpoint Alignment' if config_idx == 0 else None
                    )
                    num_plotted += 1

            self.logger.info(f"Plotted {num_plotted} data points (GC + Checkpoint metrics)")

            # Styling
            ax.set_xlabel('TaskManager Configuration', fontsize=fontsize + 2, labelpad=10)
            ax.set_ylabel('Time (s)', fontsize=fontsize + 2, labelpad=10)
            # No title - will be provided externally

            ax.set_xticks(list(range(len(labels))))
            ax.set_xticklabels(labels, fontsize=tick_size, rotation=45, ha='center')
            ax.tick_params(axis='both', labelsize=tick_size)

            # Y-axis always starts from 0
            if all_values:
                ax.set_ylim(0, 3)
            else:
                ax.set_ylim(0, 1)
                self.logger.warning("No values to plot - empty chart generated")

            ax.grid(True, linestyle='--', alpha=grid_alpha, zorder=0)

            # Format y-axis
            def format_number(x, pos):
                if abs(x) >= 1e6:
                    return f'{x / 1e6:.1f}M'
                elif abs(x) >= 1e3:
                    return f'{x / 1e3:.1f}K'
                elif abs(x) >= 1:
                    return f'{x:.1f}'
                else:
                    return f'{x:.2f}'

            ax.yaxis.set_major_formatter(FuncFormatter(format_number))

            # Add legend in top right
            handles, legend_labels = ax.get_legend_handles_labels()
            if handles:
                # Remove duplicate labels
                by_label = dict(zip(legend_labels, handles))
                ax.legend(by_label.values(), by_label.keys(),
                         fontsize=fontsize - 2,
                         loc='upper right',
                         frameon=True,
                         framealpha=0.9,
                         edgecolor='black')

            plt.tight_layout()

            save_path = self.plotter._plots_path / f"{group_key}_gc_checkpoint_metrics.png"
            plt.savefig(save_path, dpi=dpi, bbox_inches='tight')
            plt.close()

            plt.rcdefaults()

            self.logger.info(f"Generated combined GC + Checkpoint metrics plot for group {group_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate combined GC + Checkpoint plot for group {group_key}: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())

    def _plot_checkpoint_metrics(self, group_key: str, resource_data: Dict[str, Dict[str, Any]]) -> None:
        """Checkpoint metrics are now combined with GC metrics in _plot_gc_metrics."""
        # This method is no longer needed as checkpoint metrics are plotted with GC metrics
        self.logger.info(f"Checkpoint metrics for group {group_key} are combined with GC metrics plot")
        pass
