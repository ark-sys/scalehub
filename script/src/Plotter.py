import re
import tkinter as tk
from collections import defaultdict
from datetime import datetime
from tkinter import ttk, DISABLED

import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

import os
import pandas as pd
import seaborn as sns

from .utils.Config import Config, Key
from .utils.Logger import Logger
from .utils.Misc import Misc


class TreeViewFrame(tk.Frame):
    def __init__(self, parent, config: Config, log: Logger):
        super().__init__(parent)
        self.parent = parent
        self.__log = log
        self.tree = ttk.Treeview(
            self,
            columns=(
                "Name",
                "Path",
                "Date",
                "Job name",
                "Sensors",
                "Rate",
                "Load",
                "Parallelism",
                "Duration",
            ),
            displaycolumns="#all",
        )
        self.tree.column("#0", width=100)
        self.tree.column("#1", width=100)
        self.tree.column("#2", width=200)
        self.tree.column("#3", width=150)
        self.tree.column("#4", width=100)
        self.tree.column("#5", width=80)
        self.tree.column("#6", width=80)
        self.tree.column("#7", width=80)
        self.tree.column("#8", width=80)
        self.tree.column("#9", width=80)
        self.tree.heading("#1", text="Name", command=lambda: self.sort_column("Name"))
        self.tree.heading("#2", text="Path", command=lambda: self.sort_column("Path"))
        self.tree.heading("#3", text="Date", command=lambda: self.sort_column("Date"))
        self.tree.heading(
            "#4", text="Job name", command=lambda: self.sort_column("Job name")
        )
        self.tree.heading(
            "#5", text="N. Sensors", command=lambda: self.sort_column("Sensors")
        )
        self.tree.heading(
            "#6", text="Rate (s)", command=lambda: self.sort_column("Rate")
        )
        self.tree.heading(
            "#7", text="Load (sensors/s)", command=lambda: self.sort_column("Load")
        )
        self.tree.heading(
            "#8",
            text="Max Parallelism",
            command=lambda: self.sort_column("Parallelism"),
        )
        self.tree.heading(
            "#9", text="Exp Duration (s)", command=lambda: self.sort_column("Duration")
        )

        # self.sort_column("Name")
        self.tree.pack(fill="both", expand=True)

        self.populate_tree(config.get_str(Key.EXPERIMENTS_DATA_PATH))

    def is_valid_date_format(self, date_str):
        format_str = "%d-%m-%Y"
        try:
            datetime.strptime(date_str, format_str)
            return True
        except ValueError:
            return False

    def populate_tree(self, root_folder):
        self.tree.delete(*self.tree.get_children())
        self.populate_tree_dates(root_folder)

    def populate_tree_dates(self, root_folder):
        self.tree.delete(*self.tree.get_children())
        for item in os.listdir(root_folder):
            item_path = os.path.join(root_folder, item)
            if os.path.isdir(item_path) and self.is_valid_date_format(item):
                folder_name = os.path.basename(item_path)
                folder_id = self.tree.insert(
                    "",
                    "end",
                    values=(folder_name, item_path, ""),
                    open=True,
                    tags="date_directory",
                )
                self.populate_exp_number(folder_id, item_path)

    def populate_exp_number(self, parent_id, folder_path):
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isdir(item_path) and item.isdigit():
                for file in os.listdir(item_path):
                    if file.lower().endswith("stats.csv"):
                        display_file_name = "/".join(item_path.split("/")[-2:])
                        creation_date = self.get_creation_date(item_path)

                        try:
                            # Retrieve information from log file
                            m: Misc = Misc(self.__log)
                            (
                                job_name,
                                num_sensors,
                                avg_interval_ms,
                                start_ts,
                                end_ts,
                            ) = m.parse_log(os.path.join(item_path, "exp_log.txt"))
                        except FileNotFoundError:
                            job_name = "job"
                            num_sensors = 200000
                            avg_interval_ms = 3000
                            # Try at least to recover timestamps from the raw file
                            csv_files = [
                                os.path.join(item_path, raw_file)
                                for raw_file in os.listdir(item_path)
                                if raw_file.endswith(".csv") and raw_file != "stats.csv"
                            ]
                            if len(csv_files) > 0:
                                raw_file = pd.read_csv(csv_files[0])
                                start_ts = (
                                    raw_file["Time"].min() // 1000
                                    if raw_file["Time"].min() > 1000000
                                    else raw_file["Time"].min()
                                )
                                end_ts = (
                                    raw_file["Time"].max() // 1000
                                    if raw_file["Time"].max() > 1000000
                                    else raw_file["Time"].max()
                                )
                            else:
                                # Raw file not found, assign 0 to timestamps
                                start_ts = 0
                                end_ts = 0

                        ###
                        # TODO temporary solution for max parallelism
                        # Read the CSV file into a DataFrame
                        df = pd.read_csv(os.path.join(item_path, "stats.csv"))
                        # Find the maximum value in the Parallelism column
                        max_parallelism_value = df["Parallelism"].max()
                        ###
                        self.tree.insert(
                            parent_id,
                            "end",
                            values=(
                                file,
                                item_path,
                                creation_date,
                                job_name,
                                num_sensors,
                                avg_interval_ms,
                                round((num_sensors / (avg_interval_ms / 1000)), 2),
                                max_parallelism_value,
                                (end_ts - start_ts),
                            ),
                            open=True,
                            text=display_file_name,
                            tags="exp_directory",
                        )

    def get_creation_date(self, file_path):
        try:
            timestamp = os.path.getctime(file_path)
            creation_date = datetime.fromtimestamp(timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            return creation_date
        except OSError:
            return ""

    def get_selected_file(self, selected_item):
        item_values = self.tree.item(selected_item, "values")
        item_tags = self.tree.item(selected_item, "tags")
        if "date_directory" in item_tags:
            folder_path = item_values[1]
            selected_files = []
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isdir(item_path) and item.isdigit():
                    for file in os.listdir(item_path):
                        if file.lower().endswith("stats.csv"):
                            selected_files.append(os.path.join(item_path, file))
            return selected_files
        elif "exp_directory" in item_tags:
            if item_values:
                return os.path.join(
                    item_values[1], item_values[0]
                )  # Return the path of the selected file
        return None

    def sort_column(self, column):
        items = [
            (self.tree.set(child, column), child)
            for child in self.tree.get_children("")
        ]
        items.sort(
            reverse=self.tree.heading(column, "text") == "▼"
        )  # Sort in ascending order if already descending
        for index, (value, child) in enumerate(items):
            self.tree.move(child, "", index)
        self.tree.heading(
            column, text="▼" if not self.tree.heading(column, "text") else ""
        )


class PlotControlFrame(tk.Frame):
    def __init__(self, parent, tree_frame, plot_frame):
        super().__init__(parent)
        self.parent = parent
        self.tree_frame = tree_frame
        self.plot_frame = plot_frame
        self.selected_plot_types = []

        # Create checkboxes
        self.scatter_var = tk.IntVar(value=1)
        self.box_var = tk.IntVar(value=0)
        self.stacked_var = tk.IntVar(value=0)

        self.scatter_checkbox = tk.Checkbutton(
            self, text="Scatter Plot", variable=self.scatter_var
        )
        self.box_checkbox = tk.Checkbutton(
            self, text="Box Plot", variable=self.box_var, state=DISABLED
        )
        self.stacked_checkbox = tk.Checkbutton(
            self, text="Stacked Plot", variable=self.stacked_var, state=DISABLED
        )

        # Create Generate Plot button
        self.generate_button = tk.Button(
            self, text="Generate Plot", command=self.generate_plot
        )

        # Pack checkboxes and button
        self.scatter_checkbox.pack(anchor="w")
        self.box_checkbox.pack(anchor="w")
        self.stacked_checkbox.pack(anchor="w")
        self.generate_button.pack()

    def get_selected_plot_types(self):
        self.selected_plot_types = []
        if self.scatter_var.get():
            self.selected_plot_types.append("scatter")
        if self.box_var.get():
            self.selected_plot_types.append("box")
        if self.stacked_var.get():
            self.selected_plot_types.append("stacked")
        return self.selected_plot_types

    def generate_plot(self):
        def flatten(item):
            if isinstance(item, list):
                return [str(sub_item) for sub_item in item]
            else:
                return [str(item)]

        selected_files = [
            file
            for item in self.tree_frame.tree.selection()
            for file in flatten(self.tree_frame.get_selected_file(item))
        ]
        self.plot_frame.display_plot(self.get_selected_plot_types(), selected_files)


class PlotDisplayFrame(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.figure = plt.figure(figsize=(8, 6))
        self.canvas = None

    def display_plot(self, plot_types, csv_files):
        # Clear previous plot
        if self.canvas:
            self.canvas.get_tk_widget().destroy()
            self.figure.clear()

        plotter = CSVPlotter(None)  # Initialize your CSVPlotter with the CSV data
        num_subplots = len(plot_types)

        for idx, plot_type in enumerate(plot_types, start=1):
            ax = self.figure.add_subplot(num_subplots, 1, idx)
            if plot_type == "scatter":
                plotter.generate_scatter_plot(ax, csv_files)
            elif plot_type == "box":
                plotter.generate_box_plot(ax)
            elif plot_type == "stacked":
                plotter.generate_stacked_plot(ax)

        self.canvas = FigureCanvasTkAgg(self.figure, master=self)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill="both", expand=True)


class CSVPlotter:
    def __init__(self, csv_file):
        # self.data = pd.read_csv(csv_file)  # Load CSV data using pandas
        self.xlabel = "Parallelism"
        self.ylabel = "Throughput"

    def generate_scatter_plot(self, ax, csv_files):
        parent_folder_labels = [
            "/".join(os.path.dirname(csv_file).split("/")[-2:])
            for csv_file in csv_files
        ]

        # Generate a color palette using seaborn's color_palette function
        unique_dates = set(label.split("/")[0] for label in parent_folder_labels)
        palette = sns.color_palette("Set1", n_colors=len(unique_dates))

        print(f"Generating scatter plot for csv files {csv_files}")
        for csv_file, label in zip(csv_files, parent_folder_labels):
            data = pd.read_csv(csv_file)
            date_part = label.split("/")[0]
            shade_part = label.split("/")[1]
            date_color = palette[list(unique_dates).index(date_part)]
            shade_factor = (int(shade_part) - 1) / 9  # Adjust shade based on 'N'

            # Adjust color intensity by scaling the RGB components
            color = [c * (1 - shade_factor) + shade_factor for c in date_color]

            ax.scatter(data["Parallelism"], data["mean"], color=color, label=label)

        ax.set_title("Scatter Plot")
        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)
        ax.legend()

    # TODO
    def generate_box_plot(self, ax):
        self.data.boxplot(column=["value_column"], ax=ax)
        ax.set_title("Box Plot")
        ax.set_ylabel("Value")

    # TODO
    def generate_stacked_plot(self, ax):
        self.data.plot(kind="bar", stacked=True, ax=ax)
        ax.set_title("Stacked Plot")
        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ylabel)


class PlotterApp:
    def __init__(self, root, config, log):
        self.root = root
        self.root.title("Plotter App")
        self.__log = log
        # Create instances of the frames
        self.tree_frame = TreeViewFrame(self.root, config, self.__log)
        self.plot_frame = PlotDisplayFrame(self.root)
        self.control_frame = PlotControlFrame(
            self.root, self.tree_frame, self.plot_frame
        )

        # Set up the UI
        self.setup_ui()
        self.plot_frame.display_plot([], [])

    def setup_ui(self):
        # Configure grid layout
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # Pack frames
        self.tree_frame.grid(row=0, column=0, rowspan=2, sticky="nsew")
        self.control_frame.grid(row=0, column=1, sticky="nsew")
        self.plot_frame.grid(row=1, column=1, sticky="nsew")

        # Configure resizing behavior
        self.root.grid_columnconfigure(
            1, weight=1
        )  # Allow the second column to resize horizontally
        self.root.grid_rowconfigure(
            1, weight=1
        )  # Allow the second row to resize vertically

        # Bind event handler for tree item selection
        self.tree_frame.tree.bind("<<TreeviewSelect>>", self.load_selected_csv)

    def resize_vertical(self, event):
        self.root.grid_columnconfigure(0, weight=event.x_root)
        self.root.grid_columnconfigure(1, weight=self.root.winfo_width() - event.x_root)

    def resize_horizontal(self, event):
        self.root.grid_rowconfigure(0, weight=event.y_root)
        self.root.grid_rowconfigure(1, weight=self.root.winfo_height() - event.y_root)

    def load_selected_csv(self, event):
        selected_item = self.tree_frame.tree.selection()[0]
        selected_file = self.tree_frame.get_selected_file(selected_item)

        # if selected_file:
        #     self.plot_frame.display_plot(self.control_frame.get_selected_plot_types(), selected_file)


class Plotter:
    def __init__(self, config: Config, log: Logger):
        self.config = config
        self.__log = log
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", self._quit)
        self.app = PlotterApp(self.root, self.config, self.__log)
        self.root.mainloop()

    def _quit(self):
        self.root.quit()
        self.root.destroy()
        self.__log.info("Closing plotter.")
