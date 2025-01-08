import threading
from enum import Enum
from time import sleep

from transitions.extensions import LockedMachine

from scripts.monitor.experiments.Experiment import Experiment
from scripts.monitor.experiments.ResourceExperiment import ResourceExperiment
from scripts.monitor.experiments.SimpleExperiment import SimpleExperiment
from scripts.monitor.experiments.TestExperiment import TestExperiment
from scripts.monitor.experiments.TransscaleExperiment import TransscaleExperiment
from scripts.utils.Defaults import DefaultKeys as Key
from scripts.utils.Logger import Logger


# Define states of the state machine
class States(Enum):
    IDLE = "IDLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FINISHING = "FINISHING"


class ExperimentFSM(LockedMachine):

    # Define transitions of the state machine
    transitions = [
        {
            "trigger": "start_state",
            "source": States.IDLE,
            "dest": States.STARTING,
            "conditions": "configs_not_empty",
            "after": "run_state",
        },
        {
            "trigger": "run_state",
            "source": States.STARTING,
            "dest": States.RUNNING,
            "after": "finish_state",
        },
        {
            "trigger": "finish_state",
            "source": [States.RUNNING, States.STARTING],
            "dest": States.FINISHING,
            "after": "clean_state",
        },
        {
            "trigger": "clean_state",
            "source": [States.FINISHING, States.RUNNING, States.STARTING],
            "dest": States.IDLE,
            "after": "update_state",
        },
    ]

    def __init__(self, log: Logger):
        self.__log = log
        self.configs = None
        super().__init__(
            model=self,
            states=States,
            transitions=self.transitions,
            ordered_transitions=True,
            initial=States.IDLE,
        )
        self.current_experiment = None
        self.update_state_callback = None

    def configs_not_empty(self):
        return self.configs is not None and len(self.configs) > 0

    def set_configs(self, configs):
        self.__log.info(
            f"[FSM] Setting configs. Found {len(configs)} configs in sequence."
        )

        self.configs = configs

    def set_update_state_callback(self, callback):
        self.update_state_callback = callback

    def update_state(self, state_arg=States.IDLE):
        if self.update_state_callback:
            self.update_state_callback(state_arg.value)

    def __create_experiment_instance(self, config, experiment_type) -> Experiment:
        self.__log.info(
            f"[FSM] Creating experiment instance of type: {experiment_type}"
        )
        match experiment_type:
            case "transscale":
                return TransscaleExperiment(self.__log, config)
            case "simple":
                return SimpleExperiment(self.__log, config)
            case "test":
                return TestExperiment(self.__log, config)
            case "resource":
                return ResourceExperiment(self.__log, config)
            case _:
                raise ValueError(f"[FSM] Invalid experiment type: {experiment_type}")

    def __get_next_config(self):
        return self.configs.pop(0)

    def on_enter_STARTING(self):
        config = self.__get_next_config()
        experiment_type = config.get_str(Key.Experiment.type)
        self.__log.info(f"[FSM] Start phase with experiment: {experiment_type}")

        self.__log.info(f"[FSM] State is {self.state}")
        self.update_state(self.state)

        try:
            # Create experiment instance with current config
            self.current_experiment = self.__create_experiment_instance(
                config, experiment_type
            )
            self.current_experiment.starting()
            self.__log.info("[FSM] FSM startup complete, transitioning to running.")
        except Exception as e:
            self.__log.error(f"[FSM] Error while starting experiment: {str(e)}")
            self.__log.error(f"[FSM] Cleaning experiment.")

    def on_enter_RUNNING(self):
        self.__log.info("[FSM] Run phase started.")
        self.__log.info(f"[FSM] State is {self.state}")
        self.update_state(self.state)

        self.current_experiment.running()

        self.__log.info("[FSM] Run phase complete, transitioning to finishing.")

    def on_enter_FINISHING(self):
        self.__log.info("[FSM] Finish phase started.")
        self.__log.info(f"[FSM] State is {self.state}")
        self.update_state(self.state)
        if self.current_experiment:
            try:
                self.current_experiment.finishing()
                self.__log.info("[FSM] Finish phase complete.")
            except Exception as e:
                self.__log.error(f"[FSM] Error while executing end phase: {str(e)}")

    def on_enter_IDLE(self):
        # Clean flink jobs
        self.__log.info("[FSM] Clean phase started.")
        self.update_state(self.state)
        self.current_experiment.cleaning()
        self.current_experiment = None
        self.__log.info("[FSM] Clean phase complete, transitioning to idle.")


class FSMThreadWrapper(threading.Thread):
    def __init__(self, fsm: ExperimentFSM):
        super().__init__()
        self.__fsm = fsm
        self.__fsm_event = threading.Event()

    def get_fsm(self):
        return self.__fsm

    def run(self):
        while True:
            self.__fsm_event.wait()
            # An "experiment start" event has been received, start the FSM
            self.__fsm_event.clear()

            while self.__fsm.configs_not_empty():
                if self.__fsm.state == States.IDLE:
                    self.__fsm.start_state()
                    sleep(10)

    def trigger_start(self):
        self.__fsm_event.set()
