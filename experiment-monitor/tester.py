import os

from script.src.utils.Logger import Logger
from utils.Data import ExperimentData

if __name__ == "__main__":
    log = Logger()

    # base_path = "../recaps/explanation_2"
    base_path = "../paper-plots"

    # map_expe = "../paper-plots/Join-kv/latency/1"
    # exp_data = ExperimentData(log, map_expe)
    # exp_data.export_experiment_data()
    # exp_data.eval_experiment_plot()
    # exp_data.eval_summary_plot()
    # exp_data.eval_plot_with_checkpoints()
    # Iterate through all subdirectories and for all exp_log.txt files you find, create an ExperimentData object
    # Some files are 3 levels deep, some are 2 levels deep
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file == "exp_log.txt":
                exp_data = ExperimentData(log, root)

                exp_data.export_experiment_data()
                exp_data.eval_experiment_plot()
                exp_data.eval_summary_plot()
                exp_data.eval_plot_with_checkpoints()
                sleep(5)
