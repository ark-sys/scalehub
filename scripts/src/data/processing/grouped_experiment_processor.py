from typing import Dict, Any

from scripts.src.data.processing.base_processor import DataProcessor
from scripts.src.data.processing.strategies.base_processing_strategy import (
    BaseProcessingStrategy,
)
from scripts.src.data.processing.strategies.box_plot_processing_strategy import (
    BoxPlotProcessingStrategy,
)
from scripts.src.data.processing.strategies.default_multi_run_processing_strategy import (
    DefaultMultiRunProcessingStrategy,
)
from scripts.src.data.processing.strategies.experiment_group_processing_strategy import (
    ExperimentGroupProcessingStrategy,
)
from scripts.src.data.processing.strategies.resource_analysis_processing_strategy import (
    ResourceAnalysisProcessingStrategy,
)
from scripts.src.data.processing.strategies.throughput_comparison_processing_strategy import (
    ThroughputComparisonProcessingStrategy,
)
from scripts.utils.Config import Config


class GroupedExperimentProcessor(DataProcessor):
    """
    Processes data for multiple grouped experiments by delegating to specific
    processing strategies based on the experiment type.
    """

    def __init__(self, logger, config: Config, exp_path: str):
        super().__init__(logger, exp_path)
        self.config = config

    def process(self) -> Dict[str, Any]:
        """
        Determines the appropriate processing strategy, creates it, and
        executes its process method.
        """
        self.logger.info(f"Processing grouped experiment at: {self.exp_path}")
        strategy = self._create_processing_strategy()
        if strategy:
            return strategy.process()
        else:
            self.logger.warning(
                "No suitable processing strategy found. No action taken."
            )
            return {
                "type": "none",
                "message": "No suitable data or configuration found.",
            }

    def _create_processing_strategy(self) -> BaseProcessingStrategy | None:
        """
        Factory method to create the appropriate processing strategy based on
        the determined experiment type.
        """
        folder_type = self._determine_multi_exp_type()
        self.logger.info(f"Determined experiment type: {folder_type}")

        if folder_type == "experiment_group":
            return ExperimentGroupProcessingStrategy(self.logger, self.exp_path)
        elif folder_type == "box_plot_multi":
            return BoxPlotProcessingStrategy(self.logger, self.exp_path)
        elif folder_type == "throughput_comparison":
            return ThroughputComparisonProcessingStrategy(self.logger, self.exp_path)
        elif folder_type == "resource_analysis":
            return ResourceAnalysisProcessingStrategy(self.logger, self.exp_path)
        else:
            # Use default strategy for unknown experiment types
            self.logger.info("Using default multi-run processing strategy")
            return DefaultMultiRunProcessingStrategy(
                self.logger, self.exp_path, self.config
            )

    def _determine_multi_exp_type(self) -> str:
        """
        Determine what type of multi-experiment analysis to perform by inspecting
        the experiment directory's name and contents.
        """
        basename = self.exp_path.name.lower()

        # Check for experiment group pattern (a1, a2, a3, b1, b2, b3, c1, c2)
        subdirs = [d.name for d in self.exp_path.iterdir() if d.is_dir()]
        experiment_group_patterns = ['a1', 'a2', 'a3', 'b1', 'b2', 'b3', 'c1', 'c2']
        if any(pattern in subdirs for pattern in experiment_group_patterns):
            # Check if these directories contain multi-run structure (numbered subdirs)
            for subdir_name in subdirs:
                if subdir_name in experiment_group_patterns:
                    subdir_path = self.exp_path / subdir_name
                    run_dirs = [d for d in subdir_path.iterdir() if d.is_dir() and d.name.isdigit()]
                    if run_dirs:
                        self.logger.info("Detected experiment group with multi-run structure")
                        return "experiment_group"

        # Check if this is a multi_run folder with exp_log.json files in subdirectories
        run_dirs = [
            d for d in self.exp_path.iterdir() if d.is_dir() and d.name.isdigit()
        ]
        if run_dirs and any((d / "exp_log.json").exists() for d in run_dirs):
            # This is a default multi-run experiment with raw data
            return "unknown"

        # Check for resource analysis first, identified by 'resource' or 'flink' in the name
        if "resource" in basename or "flink" in basename:
            return "resource_analysis"
        # Check for box plot comparison, identified by 'tm' in subdirectory names
        elif any("tm" in d.name for d in self.exp_path.iterdir() if d.is_dir()):
            return "box_plot_multi"
        # Default to throughput comparison for other multi-folder setups with final_df files
        elif self._has_final_df_files():
            return "throughput_comparison"
        else:
            return "unknown"

    def _has_final_df_files(self) -> bool:
        """Check if final_df.csv files exist in any immediate subdirectories."""
        return any(
            (d / "final_df.csv").exists() for d in self.exp_path.iterdir() if d.is_dir()
        )
