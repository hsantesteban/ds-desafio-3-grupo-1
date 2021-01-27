import logging

from json import dumps

from os import path, makedirs

from utils.config import Config
from utils.logger import InMemoryLogger


class Api:

    # unique process identification.
    process_id = ''
    # path to store the downloaded data.
    output_path = ''
    # logger instance.
    logger = object

    def __init__(self) -> None:
        """
        Initialize API base class.
        """

        # configuration class.
        self._environment = self._init_environment()

        # instantiation flag.
        self._init = True

    @staticmethod
    def _init_environment() -> dict:
        """
        Instantiate Config class and retrieve environment file's contents.
        :return: dict
        """

        # check environment file availability.
        return Config.static_init()

    @property
    def environment(self) -> dict:
        """
        Getter for property environment.
        :return: dict
        """

        return self._environment

    def _init_logger(self, logger_name: str, file_name: str, system_logger: bool = True) -> None:
        """
        Initializes logger instance. Log is printed during execution and
        saved to a file - inside the log folder -after the execution is complete.
        :param logger_name: name of the logger which is also shared by the folder where logs are stored.
        :param file_name: name of the file that is to be stored.
        :return: None
        """

        if system_logger:
            # create logger.
            self.logger = logging.getLogger(f'{logger_name}')
            # set debug level.
            self.logger.setLevel(logging.DEBUG)
            # create file handler which logs even debug messages.
            if self.environment.get('OS_ENV', None) == 'RASPBIAN':
                file = path.join('/', 'home', 'pi', 'Documents', 'logs', logger_name, f'{file_name}.log')
            else:
                file = f'logs/{logger_name}/{file_name}.log'
            # check for location to save log files.
            makedirs(path.dirname(file), exist_ok=True)
            # file handler.
            fh = logging.FileHandler(file)
            fh.setLevel(logging.DEBUG)
            # create console handler with a higher log level
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            # create formatter and add it to the handlers
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            # add the handlers to the logger
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)
        else:
            # in memory logger.
            self.logger = InMemoryLogger()

    def _save_text_to_file(self, output_path: list, data, extension: str = 'txt', encoding: str = 'utf-8'):

        # logger.
        self.logger.info('saving text to filesystem')

        # assemble filename.
        filename = f'{path.join(*output_path)}.{extension}'

        # open file.
        with open(filename, 'w', encoding=encoding) as output:
            # write to disk.
            output.write(data)
            # logger.
            self.logger.info(f'file {"/".join(output_path)}.{extension} has been created')

    def _save_json_to_file(self, output_path: list, data: dict):

        # logger.
        self.logger.info('saving JSON to filesystem')

        # assemble filename.
        filename = f'{path.join(*output_path)}.json'

        # open file.
        with open(filename, 'w') as output:
            # write to disk.
            output.write(dumps(data))
            # logger.
            self.logger.info(f'file {"/".join(output_path)}.json has been created')
