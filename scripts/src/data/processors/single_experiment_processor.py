from typing import Dict, Any

import numpy as np
import pandas as pd

from scripts.src.data.base.data_processor import DataProcessor
from scripts.src.data.implementations.default_plotter import DefaultPlotter
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Key


class SingleExperimentProcessor(DataProcessor):
    """Processes data for a single experiment using Strategy pattern."""

    def __init__(self, logger, config: Config, exp_path: str):
        super().__init__(logger, exp_path)
        self._setup_components()

    def _setup_components(self) -> None:
        """Initialize required components."""
        log_file = self.exp_path / "exp_log.json"
        self.config = Config(self.logger, str(log_file))

        plots_path = self.exp_path / "plots"
        plots_path.mkdir(exist_ok=True)
        self.plotter = DefaultPlotter(self.logger, str(plots_path))

        self.start_skip = self.config.get_int(Key.Experiment.output_skip_s.key)
        self.end_skip = 30

    def process(self) -> Dict[str, Any]:
        """Main processing workflow - Template Method implementation."""
        raw_data = self._load_data()
        transformed_data = self._transform_data(raw_data)
        filtered_data = self._filter_data(transformed_data)
        results = self._calculate_statistics(filtered_data)

        # Save results
        output_path = self._save_results(results, "mean_stderr.csv")

        # Generate plots
        self._generate_plots(raw_data)

        return {"processed_data": results, "output_path": output_path}

    def _load_data(self) -> pd.DataFrame:
        """Load the final dataframe."""
        final_df_path = self.exp_path / "final_df.csv"
        return pd.read_csv(final_df_path, index_col=0, header=[0, 1, 2])

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and format the loaded data."""
        # Simplify column structure
        if df.columns.nlevels > 2:
            df.columns = df.columns.droplevel([1, 2])

        # Create aggregated metrics
        df["BackpressureTime"] = df.filter(
            regex="hardBackPressuredTimeMsPerSecond"
        ).mean(axis=1)
        df["BusyTime"] = df.filter(regex="busyTimeMsPerSecond").mean(axis=1)
        df["Throughput"] = df.filter(regex="numRecordsInPerSecond").sum(axis=1)

        # Convert index to datetime
        df.index = pd.to_datetime(df.index, unit="s")

        return df

    def _filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data based on time windows."""
        df_filtered = df.groupby("Parallelism").apply(
            lambda group: group[
                (
                    group.index
                    >= group.index.min() + pd.Timedelta(seconds=self.start_skip)
                )
                & (
                    group.index
                    <= group.index.max() - pd.Timedelta(seconds=self.end_skip)
                )
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
        df_final = df.groupby("Parallelism")[
            ["Throughput", "BusyTime", "BackpressureTime"]
        ].agg(["mean", lambda x: np.std(x) / np.sqrt(x.count())])

        df_final.columns = [
            "Throughput",
            "ThroughputStdErr",
            "BusyTime",
            "BusyTimeStdErr",
            "BackpressureTime",
            "BackpressureTimeStdErr",
        ]

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
            "BackpressureTime": transformed_df.filter(
                regex="hardBackPressuredTimeMsPerSecond"
            ),
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

        # Use new strategy-based approach
        self.plotter.generate_plot(
            data,
            plot_type="stacked",
            title="Experiment Plot",
            xlabel="Time (s)",
            ylabels_dict=ylabels_dict,
            ylim_dict=ylim_dict,
            filename="experiment_plot.png",
        )

    def _generate_summary_plot(self) -> None:
        """Generate summary statistics plot."""
        raw_data = self._load_data()
        transformed_data = self._transform_data(raw_data)
        filtered_data = self._filter_data(transformed_data)
        dataset = self._calculate_statistics(filtered_data)

        plot_data = {
            "ax1_data": {"Throughput": dataset["Throughput"]},
            "ax1_error_data": {"Throughput": dataset["ThroughputStdErr"]},
            "ax2_data": {
                "BusyTime": dataset["BusyTime"],
                "BackpressureTime": dataset["BackpressureTime"],
            },
            "ax2_error_data": {
                "BusyTime": dataset["BusyTimeStdErr"],
                "BackpressureTime": dataset["BackpressureTimeStdErr"],
            },
        }

        # Use new strategy-based approach
        self.plotter.generate_plot(
            plot_data,
            plot_type="single_frame",
            xlabel="Parallelism Level",
            ylabels_dict={
                "Throughput": "Records/s",
                "BusyTime": "ms/s",
                "BackpressureTime": "ms/s",
            },
            ylim=(0, (dataset["Throughput"].max() // 50000 + 1) * 50000),
            ylim2=(0, 1200),
            filename="summary_plot.png",
        )
