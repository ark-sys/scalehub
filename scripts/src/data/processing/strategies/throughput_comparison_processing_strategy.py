from typing import Dict, Any

import pandas as pd

from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)


class ThroughputComparisonProcessingStrategy(BaseProcessingStrategy):
    """Strategy for comparing throughput across different experiments."""

    def process(self) -> Dict[str, Any]:
        self.logger.info("Processing with ThroughputComparisonProcessingStrategy...")
        self._generate_multi_exp_plot()
        return {"type": "throughput_comparison"}

    def _generate_multi_exp_plot(self) -> None:
        """Generate multi-experiment throughput comparison plots."""
        machine_styles = {
            "BM": {"marker": "o", "color": "#1f77b4"},
            "VM-L": {"marker": "s", "color": "#d62728"},
            "VM-S": {"marker": "D", "color": "#2ca02c"},
        }
        # Load the processed data which contains the 'Throughput_mean' column
        experiment_data = self._load_processed_files()
        plot_data = {}
        custom_markers = {}
        custom_colors = {}

        for exp_name, df in experiment_data.items():
            # The loaded df now has 'Throughput_mean', so this will work.
            new_df = df.loc[:, ["Parallelism", "Throughput_mean"]].copy()
            new_df.rename(columns={"Throughput_mean": "Throughput"}, inplace=True)
            new_df.set_index("Parallelism", inplace=True)

            self.exporter.export_data(
                new_df, self.exp_path / f"{exp_name}_plot_data.csv"
            )

            machine_type = self._get_machine_type(exp_name)
            display_name = machine_type
            plot_data[display_name] = new_df["Throughput"]
            custom_markers[display_name] = machine_styles.get(
                machine_type, {"marker": "o"}
            )["marker"]
            custom_colors[display_name] = machine_styles.get(
                machine_type, {"color": "#000000"}
            )["color"]

        self.plotter.generate_plot(
            {
                "ax1_data": plot_data,
                "custom_markers": custom_markers,
                "custom_colors": custom_colors,
            },
            plot_type="single_frame_multiple_series",
            xlabel="Number of TaskManagers",
            ylabels_dict={"Throughput": "Throughput (records/s)"},
            filename="throughput_comparison.png",
            ylim=(0, 400000),
            axhline=350000,
        )

    def _load_processed_files(self) -> Dict[str, pd.DataFrame]:
        """Load mean_stderr.csv files from immediate subdirectories."""
        dfs = {}
        subdirs = [d for d in self.exp_path.iterdir() if d.is_dir()]
        for subdir in subdirs:
            # Corrected to load the processed summary file
            processed_file_path = subdir / "mean_stderr.csv"
            if processed_file_path.is_file():
                try:
                    df_dict = self.loader.load_data(file_path=processed_file_path)
                    dfs[subdir.name] = list(df_dict.values())[0]
                except Exception as e:
                    self.logger.error(f"Error loading {processed_file_path}: {e}")
        return dfs

    @staticmethod
    def _get_machine_type(name: str) -> str:
        """Extract standardized machine name from a string."""
        name = name.lower()
        if "bm" in name:
            return "BM"
        if "vml" in name or "vm-l" in name:
            return "VM-L"
        if "vms" in name or "vm-s" in name:
            return "VM-S"
        return name.replace("single_node_", "").replace("_", " ").title()
