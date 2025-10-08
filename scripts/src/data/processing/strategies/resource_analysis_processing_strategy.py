import re
from typing import Dict, Any

import pandas as pd

from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)


class ResourceAnalysisProcessingStrategy(BaseProcessingStrategy):
    """Strategy for analyzing resource utilization (CPU/Mem vs. Throughput)."""

    def process(self) -> Dict[str, Any]:
        self.logger.info("Processing with ResourceAnalysisProcessingStrategy...")
        resource_data = self._process_resource_data()
        self._generate_resource_plot(resource_data)
        return {"type": "resource_analysis"}

    def _process_resource_data(self) -> Dict[tuple, float]:
        """Process resource experiment data."""
        resource_data = {}
        subdirs = [d for d in self.exp_path.iterdir() if "flink" in d.name]
        for subdir in subdirs:
            final_df_path = subdir / "final_df.csv"
            try:
                df_dict = self.loader.load_data(file_path=final_df_path)
                df = list(df_dict.values())[0]
                throughput = df["Throughput_mean"].values[0]
                match = re.search(r"flink-(\d+)m-(\d+)", subdir.name)
                if match:
                    cpu, mem = int(match.group(1)) // 1000, int(match.group(2)) // 1024
                    resource_data[(cpu, mem)] = throughput
            except (FileNotFoundError, KeyError, IndexError) as e:
                self.logger.error(f"Could not process {subdir}: {e}")
        return resource_data

    def _generate_resource_plot(self, resource_data: Dict[tuple, float]) -> None:
        """Generate resource utilization plots."""
        if not resource_data:
            return
        df = pd.DataFrame(
            [
                {"cpu": cpu, "mem": mem, "throughput": throughput}
                for (cpu, mem), throughput in resource_data.items()
            ]
        )
        self.exporter.export_data(df, self.exp_path / "resource_data.csv")

        self.plotter.generate_plot(
            {"x_data": df["cpu"], "y_data": df["mem"], "z_data": df["throughput"]},
            plot_type="3d",
            xlabel="CPU (cores)",
            ylabel="Memory (GB)",
            zlabel="Throughput (Records/s)",
            filename="resource_plot_multi_run.png",
        )
