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

import re
from typing import Dict, Any

from src.scalehub.data.processing.strategies.base_processing_strategy import BaseProcessingStrategy


class BoxPlotProcessingStrategy(BaseProcessingStrategy):
    """Strategy for generating box plots from multiple final_df.csv files."""

    def process(self) -> Dict[str, Any]:
        self.logger.info("Processing with BoxPlotProcessingStrategy...")
        self._generate_box_plot_multi_exp()
        return {"type": "box_plot_multi"}

    def _generate_box_plot_multi_exp(self) -> None:
        """Generate box plot for multi-experiment comparison."""
        final_dfs = self._load_final_df_files()

        if not final_dfs:
            self.logger.error("No final_df.csv files found for box plot")
            return

        boxplot_data = []
        labels = []
        sorted_keys = sorted(final_dfs.keys())

        for exp_name in sorted_keys:
            df = final_dfs[exp_name]
            required_cols = ["Throughput_min", "Throughput_max", "Throughput_mean"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"Missing required columns in {exp_name}, skipping")
                continue

            values = [
                df["Throughput_min"].values[0],
                df["Throughput_mean"].values[0],
                df["Throughput_max"].values[0],
            ]
            boxplot_data.append(values)

            tm_match = re.match(r"(\d+)_tm_(\d+)_ts_per_tm", exp_name)
            clean_label = (
                f"{int(tm_match.group(1))} TM,\n{int(tm_match.group(2))} TS/TM"
                if tm_match
                else exp_name
            )
            labels.append(clean_label)

        if not boxplot_data:
            self.logger.error("No valid data found for box plot")
            return

        self.plotter.generate_plot(
            {
                "boxplot_data": boxplot_data,
                "labels": labels,
            },
            plot_type="whisker",
            ylim_val=400000,
            xlabel="TaskManager Configuration",
            ylabel="Throughput (records/s)",
            filename="multi_experiment_box_plot.png",
            workload_objective=350000,
        )

    def _load_final_df_files(self) -> Dict[str, Any]:
        """Load final_df.csv files from immediate subdirectories."""
        dfs = {}
        subdirs = [d for d in self.exp_path.iterdir() if d.is_dir()]
        for subdir in subdirs:
            final_df_path = subdir / "final_df.csv"
            if final_df_path.is_file():
                try:
                    df_dict = self.loader.load_data(file_path=final_df_path)
                    dfs[subdir.name] = list(df_dict.values())[0]
                except Exception as e:
                    self.logger.error(f"Error loading {final_df_path}: {e}")
        return dfs
