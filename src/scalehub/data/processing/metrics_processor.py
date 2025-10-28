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

"""
Metrics processor to handle JSON data from VictoriaMetrics and build multi-column DataFrames
similar to the old DataExporter.py functionality.
"""
from typing import Dict, List

import pandas as pd

from src.utils.Logger import Logger


class MetricsProcessor:
    """Process metrics from VictoriaMetrics JSON data to create structured DataFrames."""

    def __init__(self, logger: Logger):
        self.logger = logger

    @staticmethod
    def process_metrics(metrics_content: List[Dict], task_name: str) -> Dict[str, List[tuple]]:
        """
        Process metrics content for a specific task name.
        Returns a dict mapping subtask_index to list of (timestamp, value) tuples.
        """
        data = {}
        for metric in metrics_content:
            if "metric" not in metric:
                continue

            metric_info = metric["metric"]
            if metric_info.get("task_name") == task_name:
                subtask_index = metric_info.get("subtask_index", "0")
                if subtask_index not in data:
                    data[subtask_index] = []

                # Process values and timestamps
                values = metric.get("values", [])
                timestamps = metric.get("timestamps", [])

                for value, timestamp in zip(values, timestamps):
                    # Round timestamp to 5-second intervals
                    data[subtask_index].append((round(timestamp / 5000), value))

        return data

    def get_metrics_per_subtask(
        self, metrics_content: List[Dict], metric_name: str, task_name: str
    ) -> pd.DataFrame:
        """
        Get metrics per subtask and return as a DataFrame with multi-level columns.

        Args:
            metrics_content: List of JSON metric objects from VictoriaMetrics
            metric_name: Name of the metric
            task_name: Name of the task to filter

        Returns:
            DataFrame with multi-level columns (metric_name, task_name, subtask_index)
        """
        data = self.process_metrics(metrics_content, task_name)

        if not data:
            self.logger.warning(f"No data found for metric {metric_name} and task {task_name}")
            return pd.DataFrame()

        # Create DataFrame with one column per subtask
        df = pd.DataFrame(
            {
                f"{metric_name}_{k}": pd.Series(dict(v), name=f"{metric_name}_{k}")
                for k, v in data.items()
            }
        )

        df.sort_index(inplace=True)

        # Extract subtask indices from column names and sort
        df.columns = df.columns.astype(str).str.extract(r"(\d+)", expand=False).astype(int)
        df = df.sort_index(axis=1)

        # Create multi-level column index
        df.columns = pd.MultiIndex.from_product([[metric_name], [task_name], df.columns])

        # Scale index back to milliseconds
        df.index = df.index * 5000
        df.index.name = "Timestamp"

        return df

    def get_sources_metrics(
        self, metrics_content: List[Dict], metric_name: str
    ) -> List[pd.DataFrame]:
        """
        Get metrics for all source tasks.

        Args:
            metrics_content: List of JSON metric objects from VictoriaMetrics
            metric_name: Name of the metric

        Returns:
            List of DataFrames, one per source task
        """
        # Find all source tasks
        sources = {
            metric["metric"]["task_name"]
            for metric in metrics_content
            if "metric" in metric and "Source" in metric["metric"].get("task_name", "")
        }

        res = []
        for source in sources:
            data = self.process_metrics(metrics_content, source)

            if not data:
                continue

            df = pd.DataFrame(
                {
                    f"{metric_name}_{source}_{k}": pd.Series(dict(v), name=f"{metric_name}_{k}")
                    for k, v in data.items()
                }
            )

            df.sort_index(inplace=True)
            df.columns = df.columns.str.extract(r"(\d+)", expand=False).astype(int)
            df = df.sort_index(axis=1)
            df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])
            df.index = df.index * 5000
            df.index.name = "Timestamp"
            res.append(df)

        return res

    def build_final_dataframe(
        self,
        operator_metrics_dfs: List[pd.DataFrame],
        sources_metrics_dfs: List[pd.DataFrame],
        job_metrics_dfs: List[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Build the final DataFrame by concatenating all metrics.

        Args:
            operator_metrics_dfs: List of operator metric DataFrames
            sources_metrics_dfs: List of source metric DataFrames
            job_metrics_dfs: Optional list of job metric DataFrames

        Returns:
            Final concatenated DataFrame with Parallelism column
        """
        dfs_to_concat = []

        if operator_metrics_dfs:
            operator_metrics_df = pd.concat(
                [df for df in operator_metrics_dfs if not df.empty], axis=1
            )
            dfs_to_concat.append(operator_metrics_df)

        if sources_metrics_dfs:
            sources_metrics_df = pd.concat(
                [df for df in sources_metrics_dfs if not df.empty], axis=1
            )
            dfs_to_concat.append(sources_metrics_df)

        if job_metrics_dfs:
            job_metrics_df = pd.concat([df for df in job_metrics_dfs if not df.empty], axis=1)
            dfs_to_concat.append(job_metrics_df)

        if not dfs_to_concat:
            self.logger.warning("No data to concatenate for final DataFrame")
            return pd.DataFrame()

        final_df = pd.concat(dfs_to_concat, axis=1)

        # Normalize index to start from 0 and convert to seconds
        final_df.index = (final_df.index - final_df.index.min()) / 1000
        final_df.index = final_df.index.astype(int)

        # Calculate parallelism from numRecordsInPerSecond columns
        throughput_cols = [col for col in final_df.columns if "numRecordsInPerSecond" in str(col)]

        if throughput_cols:
            final_df["Parallelism"] = final_df[throughput_cols].count(axis=1)

        return final_df
