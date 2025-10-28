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

from typing import Dict, Any

import numpy as np
import pandas as pd

from src.scalehub.data.processing.base_processor import ProcessorWithComponents
from src.utils.Config import Config
from src.utils.Defaults import DefaultKeys as Key


class SingleExperimentProcessor(ProcessorWithComponents):
    """Processes data for a single experiment run."""

    def __init__(self, logger, config: Config, exp_path: str):
        self.config = config
        super().__init__(logger, exp_path)

    def _setup_components(self) -> None:
        """Initialize required components and config-specific settings."""
        # Call parent to setup loader, exporter, plotter
        super()._setup_components()

        # Add config-specific setup
        self.start_skip = self.config.get_int(Key.Experiment.output_skip_s.key)
        self.end_skip = 30

    def process(self) -> Dict[str, Any]:
        """Main processing workflow."""
        raw_data = self._load_data()
        transformed_data = self._transform_data(raw_data)
        filtered_data = self._filter_data(transformed_data)
        results = self._calculate_statistics(filtered_data)

        # Save results using the Exporter
        output_path = self.exp_path / "mean_stderr.csv"
        self.exporter.export_data(results, output_path)
        self.logger.info(f"Results saved to: {output_path}")

        # Generate plots
        self._generate_plots(raw_data)

        return {"processed_data": results, "output_path": str(output_path)}

    def _load_data(self) -> pd.DataFrame:
        """Load the final dataframe using the Loader."""
        final_df_path = self.exp_path / "final_df.csv"
        # The loader returns a dict; we extract the single DataFrame.
        df_dict = self.loader.load_data(file_path=final_df_path)
        df = list(df_dict.values())[0]
        # Set index and header from the original file format
        df = pd.read_csv(final_df_path, index_col=0, header=[0, 1, 2])
        return df

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and format the loaded data."""
        # Simplify column structure
        if df.columns.nlevels > 2:
            df.columns = df.columns.droplevel([1, 2])

        # Create aggregated metrics
        df["BackpressureTime"] = df.filter(regex="hardBackPressuredTimeMsPerSecond").mean(axis=1)
        df["BusyTime"] = df.filter(regex="busyTimeMsPerSecond").mean(axis=1)
        df["Throughput"] = df.filter(regex="numRecordsInPerSecond").sum(axis=1)

        # Convert index to datetime
        df.index = pd.to_datetime(df.index, unit="s")

        self.logger.debug(f"Transforming data to {df.shape}")

        return df

    def _filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data based on time windows."""
        df_filtered = df.groupby("Parallelism").apply(
            lambda group: group[
                (group.index >= group.index.min() + pd.Timedelta(seconds=self.start_skip))
                & (group.index <= group.index.max() - pd.Timedelta(seconds=self.end_skip))
            ]
        )

        if len(df_filtered) < 7:
            self.logger.warning("Filtered data has less than 7 rows, skipping filter.")
            return df

        df_filtered = df_filtered.drop(columns=["Parallelism"])
        df_filtered.reset_index(inplace=True)
        return df_filtered

    def _calculate_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate mean and standard error statistics."""
        df_final = df.groupby("Parallelism")[["Throughput", "BusyTime", "BackpressureTime"]].agg(
            ["mean", lambda x: np.std(x) / np.sqrt(x.count())]
        )

        df_final.columns = [
            "Throughput",
            "ThroughputStdErr",
            "BusyTime",
            "BusyTimeStdErr",
            "BackpressureTime",
            "BackpressureTimeStdErr",
        ]

        self.logger.debug(f"Calculated statistics:\n{df_final.head()}")

        return df_final[df_final["Throughput"] > 0]

    def _generate_plots(self, raw_data: pd.DataFrame) -> None:
        """Generate experiment and summary plots."""
        self._generate_experiment_plot(raw_data)
        self._generate_summary_plot()

    def _generate_experiment_plot(self, df: pd.DataFrame) -> None:
        """Generate time-series experiment plot."""
        transformed_df = self._transform_data(df)

        data = {
            "Throughput": transformed_df.filter(regex="numRecordsInPerSecond"),
            "BusyTime": transformed_df.filter(regex="busyTimeMsPerSecond"),
            "BackpressureTime": transformed_df.filter(regex="hardBackPressuredTimeMsPerSecond"),
        }

        ylim_dict = {
            "Throughput": (0, (data["Throughput"].max().max() // 10000 + 1) * 10000),
            "BusyTime": (0, 1200),
            "BackpressureTime": (0, 1200),
        }

        ylabels_dict = {
            "Throughput": "Records/s",
            "BusyTime": "ms/s",
            "BackpressureTime": "ms/s",
        }

        self.plotter.generate_plot(
            data,
            plot_type="stacked",
            xlabel="Time",
            ylabels_dict=ylabels_dict,
            ylim_dict=ylim_dict,
            filename="experiment_plot.png",
        )

    def _generate_summary_plot(self) -> None:
        """Generate summary plot with aggregated metrics."""
        mean_stderr_path = self.exp_path / "mean_stderr.csv"
        df_dict = self.loader.load_data(file_path=mean_stderr_path)
        df = list(df_dict.values())[0]

        self.plotter.generate_plot(
            {"x": df.index, "y": df["Throughput"], "yerr": df["ThroughputStdErr"]},
            plot_type="basic",
            xlabel="Parallelism",
            ylabel="Throughput (records/s)",
            filename="summary_plot.png",
        )
