import os
import re

from scripts.src.data.DataEval import DataEval
from scripts.src.data.DataExporter import DataExporter
from scripts.src.data.GroupedDataEval import GroupedDataEval
from scripts.utils.Config import Config
from scripts.utils.Defaults import DefaultKeys as Keys
from scripts.utils.Logger import Logger


class DataManager:
    def __init__(self, log: Logger, config: Config):
        self.__log = log
        self.__config = config

    def export(self, exp_path: str):
        # If path is absolute, use it as is otherwise append it to the base path from the config
        if not os.path.isabs(exp_path):
            exp_path = os.path.join(
                self.__config.get_str(Keys.Scalehub.experiments), exp_path
            )

        # Check that we have a dir
        if os.path.isdir(exp_path):
            self.__process_experiment_path(exp_path)
        else:
            self.__log.error(f"Invalid path: {exp_path}")

    def __process_experiment_path(self, exp_path: str):
        if self.__is_date_folder(exp_path):
            self.__process_date_folder(exp_path)
        elif self.__is_single_run_folder(exp_path):
            self.__process_single_run_folder(exp_path)
        elif self.__is_multi_run_folder(exp_path):
            self.__process_multi_run_folder(exp_path)
        elif self.__is_multi_exp_folder(exp_path):
            self.__process_multi_exp_folder(exp_path)
        else:
            self.__log.error(f"Unknown folder structure: {exp_path}")

    def __is_date_folder(self, path: str) -> bool:
        return re.match(r"^\d{4}-\d{2}-\d{2}$", os.path.basename(path)) is not None

    def __is_single_run_folder(self, path: str) -> bool:
        return re.match(r"^\d+$", os.path.basename(path)) is not None

    def __is_multi_run_folder(self, path: str) -> bool:
        return re.match(r"^multi_run_\d+$", os.path.basename(path)) is not None

    def __is_multi_exp_folder(self, path: str) -> bool:
        return re.match(r"^multi_exp_\d+$", os.path.basename(path)) is not None

    def __process_date_folder(self, date_folder: str):
        for subfolder in os.listdir(date_folder):
            subfolder_path = os.path.join(date_folder, subfolder)
            if self.__is_multi_run_folder(subfolder):
                self.__process_multi_run_folder(subfolder_path)
            elif self.__is_single_run_folder(subfolder):
                self.__process_single_run_folder(subfolder_path)

    def __process_multi_run_folder(self, multi_run_folder: str):
        for subfolder in os.listdir(multi_run_folder):
            subfolder_path = os.path.join(multi_run_folder, subfolder)
            if self.__is_single_run_folder(subfolder):
                self.__process_single_run_folder(subfolder_path)
        self.__generate_grouped_data_eval(multi_run_folder)

    def __process_single_run_folder(self, single_run_folder: str):
        self.export_experiment(single_run_folder)
        self.evaluate_experiment(single_run_folder)

    def __process_multi_exp_folder(self, multi_exp_folder: str):
        grouped_data_eval = GroupedDataEval(log=self.__log, exp_path=multi_exp_folder)

        # Detect if multi_node or single_node
        if grouped_data_eval.is_single_node():
            grouped_data_eval.generate_multi_exp_single_node_plot()
        else:
            grouped_data_eval.generate_multi_exp_multi_node_plot()

    def __generate_grouped_data_eval(self, multi_run_folder: str):
        grouped_data_eval = GroupedDataEval(log=self.__log, exp_path=multi_run_folder)
        grouped_data_eval.generate_box_for_means()

    def export_experiment(self, exp_path: str):
        data_exp = DataExporter(log=self.__log, exp_path=exp_path)
        data_exp.export()

    def evaluate_experiment(self, exp_path: str):
        data_eval = DataEval(log=self.__log, exp_path=exp_path)
        data_eval.eval_mean_stderr()
        data_eval.eval_summary_plot()
        data_eval.eval_experiment_plot()
