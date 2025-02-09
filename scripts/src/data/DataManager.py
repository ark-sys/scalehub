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

    def export(self, exp_path: str, **kwargs):
        self.__log.info(f"Exporting data from: {exp_path}")
        try:
            exp_path = (
                os.path.join(
                    self.__config.get_str(Keys.Scalehub.experiments.key), exp_path
                )
                if not os.path.isabs(exp_path)
                else exp_path
            )

            if os.path.isdir(exp_path):
                self.__process_experiment_path(exp_path, **kwargs)
            else:
                self.__log.error(f"Invalid path: {exp_path}")
                raise ValueError(f"Invalid path: {exp_path}")
        except Exception as e:
            self.__log.error(f"Error exporting data: {e}")
            raise e

    def __process_experiment_path(self, exp_path: str, **kwargs):
        try:
            self.__log.info(
                f"Processing folder: {os.path.basename(os.path.normpath(exp_path))}"
            )
            folder_type = self.__get_folder_type(exp_path)
            if folder_type == "date":
                self.__process_date_folder(exp_path, **kwargs)
            elif folder_type == "single_run":
                self.__process_single_run_folder(exp_path, **kwargs)
            elif folder_type == "res_exp":
                self.__process_res_exp_folder(exp_path, **kwargs)
            elif folder_type == "multi_run":
                self.__process_multi_run_folder(exp_path, **kwargs)
            elif folder_type == "multi_exp":
                self.__process_multi_exp_folder(exp_path, **kwargs)
            else:
                self.__log.error(f"Unknown folder structure: {exp_path}")
                raise ValueError(f"Unknown folder structure: {exp_path}")
        except Exception as e:
            self.__log.error(f"Error processing experiment path: {e}")
            raise e

    @staticmethod
    def __get_folder_type(path: str) -> str:
        basename = os.path.basename(os.path.normpath(path))

        if re.match(r"^\d{4}-\d{2}-\d{2}$", basename):
            return "date"
        elif re.match(r"^\d+$", basename):
            return "single_run"
        elif re.match(r"^multi_run_\d+$", basename) or any(
            re.match(r"^\d+$", f) for f in os.listdir(path)
        ):
            return "multi_run"
        elif re.match(r"^res_exp_\w+(_\d+)?$", basename):
            return "res_exp"
        elif re.match(r"^multi_exp_\d+(_\d+[a-zA-Z])?$", basename):
            return "multi_exp"
        else:
            return "unknown"

    def __process_date_folder(self, date_folder: str, **kwargs):
        try:
            for subfolder in os.listdir(date_folder):
                subfolder_path = os.path.join(date_folder, subfolder)
                if self.__get_folder_type(subfolder_path) == "multi_run":
                    self.__process_multi_run_folder(subfolder_path, **kwargs)
                elif self.__get_folder_type(subfolder_path) == "single_run":
                    self.__process_single_run_folder(subfolder_path, **kwargs)
        except Exception as e:
            self.__log.error(f"Error processing date folder: {e}")
            raise e

    def __process_res_exp_folder(self, res_exp_folder: str, **kwargs):
        try:
            sub_res_exp = [f for f in os.listdir(res_exp_folder) if "flink" in f]
            for subdir in sub_res_exp:
                sub_res_exp_path = os.path.join(res_exp_folder, subdir)
                self.__log.info(f"Processing subfolder: {sub_res_exp_path}")
                self.__process_multi_run_folder(sub_res_exp_path, **kwargs)

            if not kwargs.get("dry_run", False):
                self.__log.info("Generating resource plot")
                grouped_data_eval = GroupedDataEval(
                    log=self.__log, exp_path=res_exp_folder
                )
                grouped_data_eval.generate_resource_plot()
                grouped_data_eval.generate_resource_core_info()
        except Exception as e:
            self.__log.error(f"Error processing resource experiment folder: {e}")
            raise e

    def __process_multi_run_folder(self, multi_run_folder: str, **kwargs):
        try:
            for subfolder in os.listdir(multi_run_folder):
                subfolder_path = os.path.join(multi_run_folder, subfolder)
                if (
                    os.path.isdir(subfolder_path)
                    and self.__get_folder_type(subfolder_path) == "single_run"
                ):
                    self.__process_single_run_folder(subfolder_path, **kwargs)
            if not kwargs.get("dry_run", False):
                self.__generate_grouped_data_eval(multi_run_folder)
        except Exception as e:
            self.__log.error(f"Error processing multi run folder: {e}")
            raise e

    def __process_single_run_folder(self, single_run_folder: str, **kwargs):
        try:
            if not kwargs.get("dry_run", False):
                if kwargs.get("single_export", False):
                    self.export_experiment(single_run_folder)
                if kwargs.get("single_eval", True):
                    self.evaluate_experiment(single_run_folder)
        except Exception as e:
            self.__log.error(f"Error processing single run folder: {e}")
            raise e

    def __process_multi_exp_folder(self, multi_exp_folder: str, **kwargs):
        try:
            for subfolder in os.listdir(multi_exp_folder):
                subfolder_path = os.path.join(multi_exp_folder, subfolder)
                if self.__get_folder_type(subfolder_path) == "multi_run":
                    self.__process_multi_run_folder(subfolder_path, **kwargs)
            if not kwargs.get("dry_run", False):
                grouped_data_eval = GroupedDataEval(
                    log=self.__log, exp_path=multi_exp_folder
                )

                if "single_node" in multi_exp_folder:
                    grouped_data_eval.generate_multi_exp_plot()
                else:
                    grouped_data_eval.generate_multi_exp_plot(False)

        except Exception as e:
            self.__log.error(f"Error processing multi exp folder: {e}")
            raise e

    def __generate_grouped_data_eval(self, multi_run_folder: str):
        try:
            grouped_data_eval = GroupedDataEval(
                log=self.__log, exp_path=multi_run_folder
            )
            grouped_data_eval.generate_box_plot()
        except Exception as e:
            self.__log.error(f"Error generating grouped data eval: {e}")
            raise e

    def export_experiment(self, exp_path: str):
        try:
            data_exp = DataExporter(log=self.__log, exp_path=exp_path)
            data_exp.export()
        except Exception as e:
            self.__log.error(f"Error exporting data: {e}")
            raise e

    def evaluate_experiment(self, exp_path: str):
        try:
            data_eval = DataEval(log=self.__log, exp_path=exp_path)
            data_eval.eval_mean_stderr()
            data_eval.eval_summary_plot()
            data_eval.eval_experiment_plot()
        except Exception as e:
            self.__log.error(f"Error evaluating data: {e}")
            raise e
