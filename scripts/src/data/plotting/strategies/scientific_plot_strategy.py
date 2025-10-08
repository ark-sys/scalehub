"""
Advanced plot strategy for scientific publications with error bars and multi-panel layouts.
"""
from pathlib import Path
from typing import Dict, Any

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

from scripts.src.data.plotting.strategies.base_plot_strategy import PlotStrategy


class ScientificPlotStrategy(PlotStrategy):
    """
    Strategy for generating publication-quality scientific plots.
    
    Features:
    - Error bars with customizable appearance
    - Scientific styling (larger fonts, thicker lines)
    - Multiple color schemes and markers
    - Formatted axis labels (K, M notation)
    """

    def __init__(self, logger, plots_path: Path, **style_params):
        self._logger = logger
        self._plots_path = plots_path
        
        # Scientific plotting defaults
        self.figsize = style_params.get('figsize', (14, 10))
        self.fontsize = style_params.get('fontsize', 32)
        self.tick_size = style_params.get('tick_size', 32)
        self.linewidth = style_params.get('scientific_linewidth', 4)
        self.markersize = style_params.get('scientific_markersize', 25)
        self.markeredgewidth = style_params.get('scientific_markeredgewidth', 2.0)
        self.capsize = style_params.get('capsize', 10)
        self.grid_alpha = style_params.get('grid_alpha', 0.3)
        self.dpi = style_params.get('dpi', 600)
        
        # Color schemes
        self.colors = style_params.get('colors', [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
        ])
        
        self.markers = style_params.get('markers', [
            "o", "s", "^", "D", "v", "X", "P", "*", "H", "p"
        ])

    def generate(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a scientific-style plot with error bars."""
        plot_style = kwargs.get('style', 'line')  # 'line', 'bar', 'dual_axis', 'scatter'

        if plot_style == 'dual_axis':
            return self._generate_dual_axis_plot(data, **kwargs)
        elif plot_style == 'bar':
            return self._generate_bar_plot(data, **kwargs)
        elif plot_style == 'scatter':
            return self._generate_scatter_plot(data, **kwargs)
        else:
            return self._generate_line_plot(data, **kwargs)

    def _setup_style(self):
        """Configure matplotlib for scientific plotting."""
        plt.rcParams.update({
            'font.family': 'serif',
            'font.size': self.fontsize,
            'axes.labelsize': self.fontsize,
            'xtick.labelsize': self.tick_size,
            'ytick.labelsize': self.tick_size,
            'lines.linewidth': self.linewidth,
            'lines.markersize': self.markersize,
            'grid.alpha': self.grid_alpha
        })

    def _reset_style(self):
        """Reset matplotlib style."""
        plt.rcdefaults()

    def _format_number(self, x, pos):
        """Format numbers for axis labels with K/M notation."""
        if abs(x) >= 1e6:
            return f'{x / 1e6:.1f}M'
        elif abs(x) >= 1e3:
            return f'{x / 1e3:.1f}K'
        elif abs(x) >= 1:
            return f'{x:.1f}'
        else:
            return f'{x:.2f}'

    def _generate_line_plot(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a line plot with error bars."""
        self._setup_style()
        
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Extract data
        x_data = data.get('x', [])
        y_data = data.get('y', [])
        yerr = data.get('yerr', None)
        labels = data.get('labels', None)
        
        # Plot styling
        xlabel = kwargs.get('xlabel', '')
        ylabel = kwargs.get('ylabel', '')
        title = kwargs.get('title', '')
        ylim = kwargs.get('ylim', None)
        filename = kwargs.get('filename', 'scientific_plot.png')
        
        # If labels provided, use them for x-ticks
        if labels:
            x_positions = list(range(len(labels)))
            ax.set_xticks(x_positions)
            ax.set_xticklabels(labels, fontsize=self.tick_size)
            x_data = x_positions if not x_data else x_data
        
        # Plot with error bars
        ax.errorbar(
            x_data, y_data,
            yerr=yerr,
            linewidth=self.linewidth,
            marker=self.markers[0],
            markersize=self.markersize,
            markeredgecolor='black',
            markeredgewidth=self.markeredgewidth,
            color=self.colors[0],
            capsize=self.capsize,
            capthick=self.linewidth / 2,
            elinewidth=self.linewidth / 2,
            zorder=3
        )
        
        # Styling
        ax.set_xlabel(xlabel, fontsize=self.fontsize + 2, labelpad=10)
        ax.set_ylabel(ylabel, fontsize=self.fontsize + 2, labelpad=10)
        if title:
            ax.set_title(title, fontsize=self.fontsize + 2, fontweight='bold')
        
        if ylim:
            ax.set_ylim(ylim)
        
        ax.grid(True, linestyle='--', alpha=self.grid_alpha, zorder=0)
        ax.yaxis.set_major_formatter(FuncFormatter(self._format_number))
        ax.tick_params(axis='both', labelsize=self.tick_size)
        
        plt.tight_layout()
        
        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()
        
        self._reset_style()
        return save_path

    def _generate_dual_axis_plot(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a dual-axis plot for comparing two metrics."""
        self._setup_style()
        
        fig, ax1 = plt.subplots(figsize=self.figsize)
        ax2 = ax1.twinx()
        
        # Extract data
        x_data = data.get('x', [])
        y1_data = data.get('y1', [])
        y2_data = data.get('y2', [])
        y1_err = data.get('y1_err', None)
        y2_err = data.get('y2_err', None)
        labels = data.get('labels', None)
        
        # Plot styling
        xlabel = kwargs.get('xlabel', '')
        y1_label = kwargs.get('y1_label', 'Metric 1')
        y2_label = kwargs.get('y2_label', 'Metric 2')
        title = kwargs.get('title', '')
        filename = kwargs.get('filename', 'dual_axis_plot.png')
        use_lines = kwargs.get('use_lines', True)
        y1_lim = kwargs.get('y1_lim', None)
        y2_lim = kwargs.get('y2_lim', None)
        legend_loc = kwargs.get('legend_loc', 'upper right')
        markersize = kwargs.get('markersize', self.markersize)

        # Setup x-axis
        if labels:
            x_positions = list(range(len(labels)))
            ax1.set_xticks(x_positions)
            ax1.set_xticklabels(labels, fontsize=self.tick_size, rotation=45, ha='center')
            x_data = x_positions if not x_data else x_data
        
        # Plot first metric on left axis
        if use_lines:
            ax1.errorbar(
                x_data, y1_data,
                yerr=y1_err,
                linewidth=self.linewidth,
                marker=self.markers[0],
                markersize=markersize,
                markeredgecolor='black',
                markeredgewidth=self.markeredgewidth,
                color=self.colors[0],
                label=y1_label,
                capsize=self.capsize,
                capthick=self.linewidth / 2,
                elinewidth=self.linewidth / 2,
                zorder=3
            )
        else:
            # Scatter plot (no lines)
            ax1.errorbar(
                x_data, y1_data,
                yerr=y1_err,
                fmt='o',
                markersize=markersize,
                markeredgecolor='black',
                markeredgewidth=self.markeredgewidth,
                color=self.colors[0],
                label=y1_label,
                capsize=self.capsize,
                capthick=self.linewidth / 2,
                elinewidth=self.linewidth / 2,
                zorder=3
            )

        # Plot second metric on right axis
        if use_lines:
            ax2.errorbar(
                x_data, y2_data,
                yerr=y2_err,
                linewidth=self.linewidth,
                marker=self.markers[1],
                markersize=markersize,
                markeredgecolor='black',
                markeredgewidth=self.markeredgewidth,
                color=self.colors[1],
                label=y2_label,
                linestyle='--',
                capsize=self.capsize,
                capthick=self.linewidth / 2,
                elinewidth=self.linewidth / 2,
                zorder=3
            )
        else:
            # Scatter plot (no lines)
            ax2.errorbar(
                x_data, y2_data,
                yerr=y2_err,
                fmt='s',  # Square markers to differentiate from left axis
                markersize=markersize,
                markeredgecolor='black',
                markeredgewidth=self.markeredgewidth,
                color=self.colors[1],
                label=y2_label,
                capsize=self.capsize,
                capthick=self.linewidth / 2,
                elinewidth=self.linewidth / 2,
                zorder=3
            )

        # Styling
        ax1.set_xlabel(xlabel, fontsize=self.fontsize + 2, labelpad=10)
        ax1.set_ylabel(y1_label, fontsize=self.fontsize + 2, labelpad=10)
        ax2.set_ylabel(y2_label, fontsize=self.fontsize + 2, labelpad=10)
        
        if title:
            ax1.set_title(title, fontsize=self.fontsize + 2, fontweight='bold')
        
        # Set y-axis limits if provided
        if y1_lim:
            ax1.set_ylim(y1_lim)
        if y2_lim:
            ax2.set_ylim(y2_lim)

        ax1.grid(True, linestyle='--', alpha=self.grid_alpha, zorder=0)
        ax1.yaxis.set_major_formatter(FuncFormatter(self._format_number))
        ax2.yaxis.set_major_formatter(FuncFormatter(self._format_number))
        
        ax1.tick_params(axis='both', labelsize=self.tick_size)
        ax2.tick_params(axis='y', labelsize=self.tick_size)
        
        # Combined legend with custom location
        handles1, labels1 = ax1.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(handles1 + handles2, labels1 + labels2,
                  fontsize=self.fontsize,
                  loc=legend_loc,
                  frameon=True,
                  framealpha=0.9,
                  edgecolor='black')
        
        plt.tight_layout()
        
        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()
        
        self._reset_style()
        return save_path

    def _generate_bar_plot(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a bar plot with error bars."""
        self._setup_style()
        
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Extract data
        labels = data.get('labels', [])
        values = data.get('values', [])
        errors = data.get('errors', None)
        
        # Plot styling
        xlabel = kwargs.get('xlabel', '')
        ylabel = kwargs.get('ylabel', '')
        title = kwargs.get('title', '')
        ylim = kwargs.get('ylim', None)
        filename = kwargs.get('filename', 'bar_plot.png')
        
        x_positions = np.arange(len(labels))
        
        # Create bars
        bars = ax.bar(
            x_positions, values,
            yerr=errors,
            capsize=self.capsize,
            color=self.colors[0],
            edgecolor='black',
            linewidth=self.markeredgewidth,
            error_kw={'elinewidth': self.linewidth / 2, 'capthick': self.linewidth / 2},
            zorder=3
        )
        
        # Styling
        ax.set_xlabel(xlabel, fontsize=self.fontsize + 2, labelpad=10)
        ax.set_ylabel(ylabel, fontsize=self.fontsize + 2, labelpad=10)
        if title:
            ax.set_title(title, fontsize=self.fontsize + 2, fontweight='bold')
        
        ax.set_xticks(x_positions)
        ax.set_xticklabels(labels, fontsize=self.tick_size)
        
        if ylim:
            ax.set_ylim(ylim)
        
        ax.grid(True, axis='y', linestyle='--', alpha=self.grid_alpha, zorder=0)
        ax.yaxis.set_major_formatter(FuncFormatter(self._format_number))
        ax.tick_params(axis='both', labelsize=self.tick_size)
        
        plt.tight_layout()
        
        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()
        
        self._reset_style()
        return save_path

    def _generate_scatter_plot(self, data: Dict[str, Any], **kwargs) -> Path:
        """Generate a scatter plot (no lines) with error bars."""
        self._setup_style()

        fig, ax = plt.subplots(figsize=self.figsize)

        # Extract data
        x_data = data.get('x', [])
        y_data = data.get('y', [])
        yerr = data.get('yerr', None)
        labels = data.get('labels', None)

        # Plot styling
        xlabel = kwargs.get('xlabel', '')
        ylabel = kwargs.get('ylabel', '')
        title = kwargs.get('title', '')
        ylim = kwargs.get('ylim', None)
        filename = kwargs.get('filename', 'scatter_plot.png')

        # If labels provided, use them for x-ticks
        if labels:
            x_positions = list(range(len(labels)))
            ax.set_xticks(x_positions)
            ax.set_xticklabels(labels, fontsize=self.tick_size, rotation=45, ha='right')
            x_data = x_positions if not x_data else x_data

        # Scatter plot (no lines)
        ax.errorbar(
            x_data, y_data,
            yerr=yerr,
            fmt='o',  # Only markers, no lines
            markersize=self.markersize,
            markeredgecolor='black',
            markeredgewidth=self.markeredgewidth,
            color=self.colors[0],
            capsize=self.capsize,
            capthick=self.linewidth / 2,
            elinewidth=self.linewidth / 2,
            zorder=3
        )

        # Styling
        ax.set_xlabel(xlabel, fontsize=self.fontsize + 2, labelpad=10)
        ax.set_ylabel(ylabel, fontsize=self.fontsize + 2, labelpad=10)
        if title:
            ax.set_title(title, fontsize=self.fontsize + 2, fontweight='bold')

        if ylim:
            ax.set_ylim(ylim)

        ax.grid(True, linestyle='--', alpha=self.grid_alpha, zorder=0)
        ax.yaxis.set_major_formatter(FuncFormatter(self._format_number))
        ax.tick_params(axis='both', labelsize=self.tick_size)

        plt.tight_layout()

        save_path = self._plots_path / filename
        plt.savefig(save_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

        self._reset_style()
        return save_path
