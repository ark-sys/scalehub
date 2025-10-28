# Data Module Architecture

## Overview

The Data module provides a comprehensive framework for loading, processing, exporting, and visualizing experimental data from Apache Flink benchmarks. It is built around several design patterns to ensure flexibility, maintainability, and extensibility.

---

## Design Patterns

### 1. **Strategy Pattern**
Used for interchangeable algorithms in three domains:
- **Loading**: Different data sources (files, VictoriaMetrics, mock data)
- **Exporting**: Different output formats (CSV, potentially JSON, etc.)
- **Plotting**: Different visualization types (basic, stacked, 3D, whisker, etc.)

### 2. **Factory Pattern**
Used for creating appropriate processors and strategies:
- **ProcessorFactory**: Creates the right processor based on experiment structure
- **PlotStrategyFactory**: Creates the right plotting strategy based on plot type

### 3. **Template Method Pattern**
Used in base classes to define processing workflow:
- **DataProcessor**: Defines abstract `process()` method
- **BaseProcessingStrategy**: Provides common setup for grouped experiments

---

## Module Structure

```
data/
├── manager.py                    # Entry point for data operations
├── loading/                      # Data loading components
│   ├── loader.py                # Context for loading strategies
│   └── strategies/
│       ├── base_load_strategy.py
│       ├── file_load_strategy.py          # Load from local CSV files
│       ├── victoria_metrics_load_strategy.py  # Load from metrics DB
│       └── mock_load_strategy.py          # Load mock data for testing
├── exporting/                    # Data export components
│   ├── exporter.py              # Context for export strategies
│   └── strategies/
│       ├── base_export_strategy.py
│       └── csv_export_strategy.py         # Export to CSV format
├── plotting/                     # Visualization components
│   ├── plotter.py               # Abstract plotter interface
│   ├── default_plotter.py       # Default plotter implementation
│   ├── factory.py               # Factory for plot strategies
│   └── strategies/
│       ├── base_plot_strategy.py
│       ├── basic_plot_strategy.py         # Simple line plots
│       ├── stacked_plot_strategy.py       # Multiple stacked subplots
│       ├── single_frame_plot_strategy.py  # Single frame with multiple series
│       ├── stacked_frames_plot_strategy.py
│       ├── whisker_plot_strategy.py       # Box/whisker plots
│       └── three_d_plot_strategy.py       # 3D surface plots
└── processing/                   # Data processing components
    ├── factory.py               # Factory for creating processors
    ├── base_processor.py        # Abstract base for all processors
    ├── single_experiment_processor.py      # Process single run
    ├── grouped_experiment_processor.py     # Process multiple runs
    ├── metrics_processor.py     # Transform VictoriaMetrics JSON data
    └── strategies/
        ├── base_processing_strategy.py
        ├── default_multi_run_processing_strategy.py  # Default multi-run analysis
        ├── box_plot_processing_strategy.py           # TaskManager config comparison
        ├── throughput_comparison_processing_strategy.py  # Machine type comparison
        └── resource_analysis_processing_strategy.py  # CPU/Memory analysis
```

---

## Component Descriptions

### Entry Point

#### `DataManager`
- **Purpose**: Main entry point for all data operations
- **Responsibilities**: 
  - Coordinates the entire data pipeline
  - Resolves experiment paths (relative or absolute)
  - Uses `ProcessorFactory` to create appropriate processor
- **Usage**:
  ```python
  dm = DataManager(logger, config)
  dm.export("2024-10-04/multi_run_1")
  ```

---

### Loading Components

#### `Loader` (Context)
- **Purpose**: Context class for Strategy Pattern
- **Responsibilities**: Executes loading using injected strategy
- **Methods**:
  - `load_data(**kwargs) -> Dict[str, pd.DataFrame]`: Load data using current strategy
  - `set_strategy(strategy)`: Switch loading strategy dynamically

#### Loading Strategies

##### `FileLoadStrategy`
- **Purpose**: Load data from local CSV files
- **Use Case**: Loading processed experiment results (final_df.csv, mean_stderr.csv)
- **Returns**: Dict with filename as key, DataFrame as value

##### `VictoriaMetricsLoadStrategy`
- **Purpose**: Load time-series metrics from VictoriaMetrics database
- **Use Case**: Fetching raw metrics from monitoring system during multi-run processing
- **Features**:
  - Supports both CSV and JSON export formats
  - Automatic fallback URLs if primary connection fails
  - Loads predefined metric sets (throughput, CPU, memory, checkpointing, etc.)
- **Parameters**: `db_url`, `start_ts`, `end_ts`

##### `MockLoadStrategy`
- **Purpose**: Load mock data for testing
- **Use Case**: Unit tests and development without real data

---

### Exporting Components

#### `Exporter` (Context)
- **Purpose**: Context class for Strategy Pattern
- **Responsibilities**: Executes export using injected strategy
- **Methods**:
  - `export_data(data: pd.DataFrame, output_path: Path)`: Export data using current strategy
  - `set_strategy(strategy)`: Switch export strategy dynamically

#### Export Strategies

##### `CsvExportStrategy`
- **Purpose**: Export DataFrames to CSV format
- **Features**:
  - Automatic directory creation
  - Preserves index and multi-level columns
- **Use Case**: Standard output format for all processed data

---

### Plotting Components

#### `PlotterInterface` (Abstract)
- **Purpose**: Defines interface for all plotters
- **Abstract Method**: `generate_plot(data: Dict[str, Any], **kwargs) -> Path`

#### `DefaultPlotter`
- **Purpose**: Default implementation using strategy factory
- **Responsibilities**:
  - Uses `PlotStrategyFactory` to create appropriate plot strategy
  - Manages plot output directory
  - Applies consistent styling across all plots
- **Usage**:
  ```python
  plotter = DefaultPlotter(logger, plots_path)
  plotter.generate_plot(data, plot_type="stacked", xlabel="Time", ylabel="Value")
  ```

#### `PlotStrategyFactory`
- **Purpose**: Factory for creating plot strategies
- **Available Strategies**:
  - `basic`: Simple line plots with optional error bars
  - `stacked`: Multiple stacked subplots sharing x-axis
  - `single_frame`: Multiple series on same axes
  - `whisker`: Box/whisker plots for distribution comparison
  - `3d`: 3D surface plots for resource analysis
  - `stacked_frames`: Multiple independent frames
- **Extensible**: New strategies can be registered via `register_strategy()`

#### Plot Strategies

All plot strategies implement `PlotStrategy.generate(data, **kwargs)` and return the saved plot path.

##### `BasicPlotStrategy`
- **Purpose**: Single line or scatter plot
- **Data Format**: `{"x": [...], "y": [...], "yerr": [...]}` or direct plottable data
- **Options**: `title`, `xlabel`, `ylabel`, `ylim`, `axhline`, `filename`

##### `StackedPlotStrategy`
- **Purpose**: Multiple metrics in vertically stacked subplots
- **Data Format**: `{"Metric1": Series, "Metric2": Series, ...}`
- **Options**: `ylabels_dict`, `ylim_dict` (per-subplot limits)
- **Use Case**: Comparing throughput, busy time, and backpressure over time

##### `WhiskerPlotStrategy`
- **Purpose**: Box plots for comparing distributions
- **Data Format**: `{"boxplot_data": [[min, median, max], ...], "labels": [...]}`
- **Options**: `ylim_val`, `workload_objective` (horizontal reference line)
- **Use Case**: TaskManager configuration comparison

##### `ThreeDPlotStrategy`
- **Purpose**: 3D surface plots
- **Data Format**: `{"x_data": [...], "y_data": [...], "z_data": [...]}`
- **Options**: `xlabel`, `ylabel`, `zlabel`
- **Use Case**: CPU/Memory vs Throughput analysis

---

### Processing Components

#### `ProcessorFactory`
- **Purpose**: Factory for creating appropriate data processor
- **Decision Logic**:
  1. Analyzes folder name pattern (date, numeric, multi_run_*, res_exp_*, etc.)
  2. Checks folder contents (subdirectories, exp_log.json files)
  3. Returns appropriate processor type
- **Processor Types**:
  - `SingleExperimentProcessor`: For individual run directories (numeric names)
  - `GroupedExperimentProcessor`: For multi-run experiments or comparisons

#### `DataProcessor` (Abstract Base)
- **Purpose**: Base class for all processors
- **Responsibilities**:
  - Path validation
  - Defines abstract `process()` method
  - Provides logger and path access
- **Subclass**: `ProcessorWithComponents`

#### `ProcessorWithComponents` (Concrete Base)
- **Purpose**: Provide common component setup for all processors
- **Provides**: 
  - `self.loader`: Loader with FileLoadStrategy
  - `self.exporter`: Exporter with CsvExportStrategy
  - `self.plotter`: DefaultPlotter for visualization
  - `_setup_components()`: Overridable method for custom setup
- **Subclasses**: `SingleExperimentProcessor`, `BaseProcessingStrategy`

#### `SingleExperimentProcessor`
- **Purpose**: Process a single experimental run
- **Input**: Directory containing `final_df.csv` (multi-column time-series data)
- **Output**: 
  - `mean_stderr.csv`: Aggregated statistics per parallelism level
  - `plots/experiment_plot.png`: Time-series visualization
  - `plots/summary_plot.png`: Aggregated metrics visualization
- **Workflow**:
  1. Load `final_df.csv` (multi-level columns: metric, task, subtask)
  2. Transform data (simplify columns, create aggregated metrics)
  3. Filter data (skip initial warmup and final cooldown periods)
  4. Calculate statistics (mean, stderr per parallelism level)
  5. Export results and generate plots

#### `GroupedExperimentProcessor`
- **Purpose**: Process multiple related experiments
- **Responsibilities**:
  - Determines experiment type via `_determine_multi_exp_type()`
  - Creates appropriate processing strategy
  - Delegates processing to strategy
- **Workflow**:
  1. Analyze folder structure and naming conventions
  2. Select appropriate processing strategy
  3. Execute strategy's `process()` method

#### `BaseProcessingStrategy` (Abstract)
- **Purpose**: Base class for grouped experiment processing strategies
- **Extends**: `ProcessorWithComponents`
- **Provides**:
  - Common setup: `loader`, `exporter`, `plotter`
  - Abstract `process()` method
- **Subclasses**: All grouped processing strategies

#### Processing Strategies

##### `DefaultMultiRunProcessingStrategy`
- **Purpose**: Default handler for multi-run experiments with raw data
- **Trigger**: Folders with numeric subdirectories containing `exp_log.json`
- **Workflow**:
  1. For each run directory:
     - Load timestamps from `exp_log.json`
     - Fetch raw metrics from VictoriaMetrics using `VictoriaMetricsLoadStrategy`
     - Export raw metrics to CSV/JSON files
     - Build `final_df.csv` using `MetricsProcessor`
     - Process run using `SingleExperimentProcessor` to generate `mean_stderr.csv`
  2. Aggregate results across all runs
  3. Generate summary plots showing mean/min/max across runs
- **Output**:
  - Per-run: `final_df.csv`, `mean_stderr.csv`, plots
  - Multi-run: `final_df.csv`, `aggregated_results.csv`, summary plots

##### `BoxPlotProcessingStrategy`
- **Purpose**: Compare different TaskManager configurations using box plots
- **Trigger**: Subdirectories with "tm" in name (e.g., `4_tm_8_ts_per_tm`)
- **Input**: `final_df.csv` in each subdirectory
- **Output**: `multi_experiment_box_plot.png` showing throughput distributions
- **Use Case**: Determine optimal TaskManager/TaskSlot configuration

##### `ThroughputComparisonProcessingStrategy`
- **Purpose**: Compare throughput across different machine types
- **Trigger**: Multiple subdirectories with `final_df.csv` files (not matching other patterns)
- **Input**: `mean_stderr.csv` in each subdirectory
- **Output**: `throughput_comparison.png` with multiple series (BM, VM-L, VM-S)
- **Use Case**: Compare bare metal vs different VM configurations

##### `ResourceAnalysisProcessingStrategy`
- **Purpose**: Analyze CPU/Memory resource utilization vs throughput
- **Trigger**: Folders with "resource" or "flink" in name
- **Input**: `final_df.csv` in subdirectories named like `flink-4000m-8Gi`
- **Output**: 
  - `resource_plot_multi_run.png`: 3D plot (CPU, Memory, Throughput)
  - `resource_data.csv`: Processed resource metrics
  - Optional LaTeX output
- **Use Case**: Determine optimal resource allocation

#### `MetricsProcessor`
- **Purpose**: Transform VictoriaMetrics JSON data into structured DataFrames
- **Responsibilities**:
  - Parse JSON metrics from VictoriaMetrics
  - Create multi-level column structure (metric, task, subtask)
  - Aggregate timestamp intervals (5-second buckets)
  - Build final DataFrame with Parallelism column
- **Key Methods**:
  - `get_metrics_per_subtask()`: Extract metrics for specific task
  - `get_sources_metrics()`: Extract metrics for all source operators
  - `build_final_dataframe()`: Combine all metrics into final structure
- **Output Format**: DataFrame with multi-level columns compatible with `SingleExperimentProcessor`

---

## Data Flow

### Single Experiment Processing
```
User Command
    ↓
DataManager.export()
    ↓
ProcessorFactory.create_processor()
    ↓
SingleExperimentProcessor.process()
    ↓
┌─────────────────────────────────────────┐
│ 1. Loader.load_data()                   │
│    └─ FileLoadStrategy                  │
│       └─ Load final_df.csv              │
│                                          │
│ 2. Transform & Filter Data              │
│    └─ Simplify columns                  │
│    └─ Create aggregated metrics         │
│    └─ Apply time window filters         │
│                                          │
│ 3. Calculate Statistics                 │
│    └─ Group by Parallelism              │
│    └─ Compute mean & stderr             │
│                                          │
│ 4. Exporter.export_data()               │
│    └─ CsvExportStrategy                 │
│       └─ Save mean_stderr.csv           │
│                                          │
│ 5. DefaultPlotter.generate_plot()       │
│    └─ PlotStrategyFactory               │
│       ├─ StackedPlotStrategy            │
│       └─ BasicPlotStrategy              │
└─────────────────────────────────────────┘
```

### Multi-Run Experiment Processing
```
User Command
    ↓
DataManager.export()
    ↓
ProcessorFactory.create_processor()
    ↓
GroupedExperimentProcessor.process()
    ↓
_determine_multi_exp_type()
    ↓
DefaultMultiRunProcessingStrategy.process()
    ↓
┌─────────────────────────────────────────┐
│ FOR EACH RUN:                           │
│   1. Load exp_log.json                  │
│      └─ Extract timestamps & config     │
│                                          │
│   2. Loader.load_data()                 │
│      └─ VictoriaMetricsLoadStrategy     │
│         ├─ Fetch CSV format (raw)       │
│         └─ Fetch JSON format (detailed) │
│                                          │
│   3. Export raw metrics                 │
│      └─ Save to run_N/export/           │
│                                          │
│   4. MetricsProcessor.build_final_df()  │
│      └─ Parse JSON metrics              │
│      └─ Create multi-level structure    │
│      └─ Save final_df.csv               │
│                                          │
│   5. SingleExperimentProcessor.process()│
│      └─ Generate mean_stderr.csv        │
│      └─ Generate per-run plots          │
│                                          │
│ AGGREGATE ACROSS RUNS:                  │
│   6. Combine all mean_stderr.csv        │
│      └─ Group by Parallelism            │
│      └─ Calculate mean/std/min/max      │
│                                          │
│   7. Export aggregated results          │
│      └─ Save final_df.csv (multi-run)   │
│                                          │
│   8. Generate summary plots             │
│      └─ Throughput with error bars      │
│      └─ Time metrics (stacked)          │
│      └─ Resource metrics (optional)     │
└─────────────────────────────────────────┘
```

---

## Usage Examples

### Basic Data Export

```python
from src.utils.Logger import Logger
from src.utils.Config import Config
from src.scalehub.data.manager import DataManager

# Setup
logger = Logger()
config = Config(logger, "conf/defaults.ini")

# Export single experiment
dm = DataManager(logger, config)
dm.export("2024-10-04/1")  # Relative path from experiments directory
```

### Custom Loading Strategy

```python
from src.scalehub.data.loading.loader import Loader
from src.scalehub.data.loading.strategies.victoria_metrics_load_strategy import VictoriaMetricsLoadStrategy

# Load from VictoriaMetrics
vm_strategy = VictoriaMetricsLoadStrategy(
    logger,
    db_url="vm.scalehub.dev",
    start_ts="1696435200",
    end_ts="1696438800"
)
loader = Loader(vm_strategy)
data = loader.load_data(format="json")
```

### Custom Plot Generation

```python
from src.scalehub.data.plotting.default_plotter import DefaultPlotter

plotter = DefaultPlotter(logger, "output/plots")

# Basic line plot
plotter.generate_plot(
    {
        "x": [1, 2, 3, 4],
        "y": [10, 20, 15, 25]},
    plot_type="basic",
    xlabel="Parallelism",
    ylabel="Throughput (records/s)",
    filename="custom_plot.png"
)

# Stacked subplots
plotter.generate_plot(
    {
        "Throughput": throughput_series,
        "BusyTime": busytime_series
    },
    plot_type="stacked",
    xlabel="Time (s)",
    ylabels_dict={
        "Throughput": "records/s",
        "BusyTime": "ms/s"},
    filename="metrics_over_time.png"
)
```

---

## Extension Points

### Adding New Load Strategy

```python
from src.scalehub.data.loading.strategies.base_load_strategy import BaseLoadStrategy


class CustomLoadStrategy(BaseLoadStrategy):
    def load(self, **kwargs) -> Dict[str, pd.DataFrame]:
        # Implement custom loading logic
        return {
            "data_key": dataframe}


# Usage
loader = Loader(CustomLoadStrategy(logger))
data = loader.load_data(custom_param="value")
```

### Adding New Processing Strategy

```python
from src.scalehub.data.processing.strategies.base_processing_strategy import BaseProcessingStrategy


class CustomProcessingStrategy(BaseProcessingStrategy):
    def process(self) -> Dict[str, Any]:
        # Load data
        data = self.loader.load_data(...)

        # Process data
        results = self._custom_processing(data)

        # Export results
        self.exporter.export_data(results, self.exp_path / "output.csv")

        # Generate plots
        self.plotter.generate_plot(...)

        return {
            "type": "custom",
            "status": "success"}
```

### Adding New Plot Strategy

```python
from src.scalehub.data.plotting.strategies.base_plot_strategy import PlotStrategy
from src.scalehub.data.plotting.factory import PlotStrategyFactory


class CustomPlotStrategy(PlotStrategy):
    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        # Implement custom plotting logic
        # Save plot and return path
        return plot_path


# Register new strategy
PlotStrategyFactory.register_strategy("custom", CustomPlotStrategy)

# Usage
plotter.generate_plot(data, plot_type="custom", ...)
```

---

## Pattern Analysis

### ✅ Patterns Properly Implemented

1. **Strategy Pattern** (Loading/Exporting/Plotting)
   - Clear separation of algorithm from context
   - Easy to add new strategies without modifying existing code
   - Runtime strategy switching supported

2. **Factory Pattern** (Processors/Plot Strategies)
   - Centralized object creation logic
   - Easy to extend with new processor types
   - Type detection logic isolated in factory

3. **Template Method Pattern** (Base Processors)
   - Abstract base classes define workflow
   - Concrete classes implement specific steps
   - Consistent interface across implementations

4. **Unified Processor Hierarchy** ✨ **(Recently Refactored)**
   - All processors now extend from a common base (`DataProcessor`)
   - Component setup (loader, exporter, plotter) consolidated in `ProcessorWithComponents`
   - Both `SingleExperimentProcessor` and `BaseProcessingStrategy` inherit from `ProcessorWithComponents`
   - Code duplication eliminated while maintaining flexibility

### ✅ Architecture Improvements (Implemented)

#### Unified Inheritance Hierarchy

**Previous Structure:**
```
DataProcessor (abstract)
└── SingleExperimentProcessor
    └── duplicated: loader, exporter, plotter setup

BaseProcessingStrategy (not extending DataProcessor)
└── duplicated: loader, exporter, plotter setup
```

**Current Structure:**
```
DataProcessor (abstract)
└── ProcessorWithComponents (provides common setup)
    ├── SingleExperimentProcessor
    │   └── overrides _setup_components() for config-specific settings
    └── BaseProcessingStrategy (abstract)
        ├── BoxPlotProcessingStrategy
        ├── ThroughputComparisonProcessingStrategy
        ├── ResourceAnalysisProcessingStrategy
        └── DefaultMultiRunProcessingStrategy
```

**Benefits:**
- ✅ **DRY Principle**: Component setup code exists in only one place
- ✅ **Maintainability**: Changes to setup logic only need to be made once
- ✅ **Consistency**: All processors use the same initialization pattern
- ✅ **Polymorphism**: All processors can be treated uniformly through base class
- ✅ **Extensibility**: New processors automatically get standard components
- ✅ **Testability**: Easy to mock components at the base level

---

## Class Hierarchy Details

### `DataProcessor` (Abstract Base)
- **Purpose**: Define minimal interface for all processors
- **Provides**: Path validation, logger access, abstract `process()` method
- **Subclass**: `ProcessorWithComponents`

### `ProcessorWithComponents` (Concrete Base)
- **Purpose**: Provide common component setup for all processors
- **Provides**: 
  - `self.loader`: Loader with FileLoadStrategy
  - `self.exporter`: Exporter with CsvExportStrategy
  - `self.plotter`: DefaultPlotter for visualization
  - `_setup_components()`: Overridable method for custom setup
- **Subclasses**: `SingleExperimentProcessor`, `BaseProcessingStrategy`

### `SingleExperimentProcessor`
- **Purpose**: Process individual experiment runs
- **Extends**: `ProcessorWithComponents`
- **Customization**: Overrides `_setup_components()` to add config-specific settings (start_skip, end_skip)

### `BaseProcessingStrategy` (Abstract)
- **Purpose**: Base for grouped experiment processing strategies
- **Extends**: `ProcessorWithComponents`
- **Subclasses**: All grouped processing strategies

---

## Recommendations


### 3. Consider Dependency Injection (Optional Enhancement)

For even greater flexibility and testability, consider allowing component injection:

```python
class ProcessorWithComponents(DataProcessor):
    def __init__(
        self, 
        logger: Logger, 
        exp_path: str,
        loader: Loader = None,
        exporter: Exporter = None,
        plotter: PlotterInterface = None
    ):
        super().__init__(logger, exp_path)
        self.loader = loader
        self.exporter = exporter
        self.plotter = plotter
        if not all([loader, exporter, plotter]):
            self._setup_components()
    
    def _setup_components(self) -> None:
        """Initialize components only if not injected."""
        if not self.loader:
            self.loader = Loader(FileLoadStrategy(self.logger))
        if not self.exporter:
            self.exporter = Exporter(CsvExportStrategy(self.logger))
        if not self.plotter:
            plots_path = self.exp_path / "plots"
            plots_path.mkdir(exist_ok=True)
            self.plotter = DefaultPlotter(self.logger, str(plots_path))
```

**Benefits:**
- Easier unit testing with mock components
- Ability to use custom strategies without modifying processor classes
- Runtime flexibility for different processing scenarios

**Note**: This is an optional enhancement. The current implementation with `ProcessorWithComponents` already provides significant improvements in code organization and maintainability.
