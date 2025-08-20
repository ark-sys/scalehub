from scripts.utils.Config import Config
from scripts.utils.Logger import Logger
from .factories.processor_factory import ProcessorFactory


class DataManager:
    def __init__(self, logger: Logger, config: Config):
        self._logger = logger
        self._config = config
        self._processor_factory = ProcessorFactory(logger, config)

    def export(
        self,
        exp_path: str,
        dry_run: bool = False,
        single_export: bool = False,
        single_eval: bool = False,
    ):
        # Implementation using new architecture
        processor = self._processor_factory.create_processor(exp_path)
        processor.process(dry_run, single_export, single_eval)
