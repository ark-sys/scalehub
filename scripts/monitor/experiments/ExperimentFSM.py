from enum import Enum

from transitions import Machine

from scripts.monitor.experiments.Experiment import Experiment
from scripts.monitor.experiments.SimpleExperiment import SimpleExperiment
from scripts.monitor.experiments.StandaloneExperiment import StandaloneExperiment
from scripts.monitor.experiments.TransscaleExperiment import TransscaleExperiment
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


class ExperimentFSM:
    # Define states of the state machine
    class States(Enum):
        IDLE = "IDLE"
        STARTING = "STARTING"
        RUNNING = "RUNNING"
        FINISHING = "FINISHING"

    # Define transitions of the state machine
    transitions = [
        {
            "trigger": "start",
            "source": States.IDLE,
            "dest": States.STARTING,
        },
        {
            "trigger": "run",
            "source": States.STARTING,
            "dest": States.RUNNING,
        },
        {
            "trigger": "finish",
            "source": States.RUNNING,
            "dest": States.FINISHING,
        },
        {
            "trigger": "clean",
            "source": "*",
            "dest": States.IDLE,
        },
    ]

    def __init__(self, log: Logger):
        self.__log = log
        self.machine = Machine(
            model=self,
            states=self.States,
            transitions=self.transitions,
            initial=self.States.IDLE,
        )

        self.machine.on_enter_STARTING("start_experiment")
        self.machine.on_enter_RUNNING("run_experiment")
        self.machine.on_enter_FINISHING("end_experiment")
        self.machine.on_enter_IDLE("clean_experiment")

        # This holds the current experiment instance
        self.current_experiment = None
        self.update_state_callback = None

    def _set_config(self, config):
        self.__log.info("[FSM] Setting config.")
        self.config = config

    def _set_update_state_callback(self, callback):
        self.update_state_callback = callback

    def create_experiment_instance(self, experiment_type) -> Experiment:
        self.__log.info(
            f"[FSM] Creating experiment instance of type: {experiment_type}"
        )
        match experiment_type:
            case "standalone":
                return StandaloneExperiment(self.__log, self.config)
            case "transscale":
                return TransscaleExperiment(self.__log, self.config)
            case "simple":
                return SimpleExperiment(self.__log, self.config)
            case _:
                raise ValueError(f"[FSM] Invalid experiment type: {experiment_type}")

    def start_experiment(self):
        experiment_type = self.config.get_str(Key.Experiment.type)
        self.__log.info(f"[FSM] Start phase with experiment: {experiment_type}")

        self.__log.info(f"[FSM] State is {self.state}")

        try:
            # Create experiment instance with current config
            self.current_experiment = self.create_experiment_instance(experiment_type)
            self.current_experiment.start()
            self.__log.info("[FSM] FSM startup complete, transitioning to running.")
            self.to_RUNNING()
        except Exception as e:
            self.__log.error(f"[FSM] Error while starting experiment: {e}")
            self.__log.error(f"[FSM] Cleaning experiment.")
            self.to_IDLE()

    def run_experiment(self):
        self.__log.info("[FSM] Run phase started.")

        self.current_experiment.running()

        self.__log.info("[FSM] Run phase complete, transitioning to finishing.")
        self.to_FINISHING()

    def end_experiment(self):
        if self.current_experiment:
            try:
                self.__log.info("[FSM] Finish phase started.")
                self.current_experiment.stop()
                self.__log.info("[FSM] Finish phase complete.")
            except Exception as e:
                self.__log.error(f"[FSM] Error while executing end phase: {e}")
            finally:
                # Transitioning to clean
                self.to_IDLE()

    def clean_experiment(self):
        # Clean flink jobs
        self.__log.info("[FSM] Clean phase started.")
        if self.current_experiment:
            self.current_experiment.cleanup()
            self.current_experiment = None
        self.__log.info("[FSM] Clean phase complete, transitioning to idle.")
        self.to_IDLE()
