from scripts.utils.Config import Config
from scripts.utils.Logger import Logger
from scripts.src.data.processing.factory import ProcessorFactory
import os


class DataManager:
    def __init__(self, logger: Logger, config: Config):
        self._logger = logger
        self._config = config
        self._processor_factory = ProcessorFactory(logger, config)

    def export(
        self,
        exp_path: str,
    ):
        # Retrieve base experiments directory from config if exp_path is not absolute
        if not os.path.isabs(exp_path):
            base_dir = self._config.get_str("scalehub.experiments")
            exp_path = os.path.join(base_dir, exp_path)
        processor = self._processor_factory.create_processor(exp_path)
        processor.process()
