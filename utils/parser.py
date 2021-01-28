from os.path import join, isfile, splitext
from os import listdir, remove

from json import loads

from utils.logger import InMemoryLogger
from utils.config import Config


class Parser:

    def __init__(self):

        # initialise logger.
        self.logger: InMemoryLogger = InMemoryLogger()

        # initialise config
        self._environment: dict = self._init_environment()

        # instantiation flag.
        self._init: bool = True

        # initialise containers.
        self._raw_data_container: list = list()
        self._files_container: list = list()

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

    @environment.setter
    def environment(self, env: dict) -> None:
        """
        Property setter for environment.
        :param env: desired dictionary object for environment property.
        :return: None
        """
        # check input for environment.
        if not isinstance(env, dict):
            raise ValueError('environment should be type dict')
        # check for changes after instantiation.
        if self._init:
            raise ValueError('environment variables cannot be altered at runtime')

        # assign value.
        self._environment = env

    def _clear_containers(self):
        # wipe data from containers.
        self._raw_data_container.clear()
        self._files_container.clear()

    def _read_files(self, path: str, allowed_extensions: tuple = ('.csv', '.txt', '.json', '.html'),
                    list_only: bool = False, max_files: int = 99999, qualifier: str = '*'):

        # iterate files in directory.
        for file in listdir(path):

            # check for file.
            if not isfile(join(path, file)):
                # skip directories.
                continue

            # check file extension.
            file_extension = splitext(file)[1]
            if file_extension not in allowed_extensions:
                # skip file.
                continue

            # check qualifier.
            if qualifier not in ('', '*', None):
                # filter file by qualifier.
                if qualifier not in file:
                    # skip file
                    continue

            # open file.
            with open(join(path, file)) as f:
                # check for listing.
                if not list_only:
                    # check reader type.
                    if file_extension in ('.csv', '.txt', '.html'):
                        # append data to files container.
                        self._raw_data_container.append(join(path, file))
                    elif file_extension == '.json':
                        # append data to files container.
                        self._raw_data_container.append(loads(f.read()))

                # append file path to files container.
                self._files_container.append(join(path, file))

            # check max files limit.
            if len(self._raw_data_container) >= max_files:
                # interrupt reading due to limit reach.
                break

    def _remove_files(self):

        # logger.
        self.logger.info('initialising file removal routine')

        # iterate files container.
        for file in self._files_container:
            # remove file.
            remove(file)

        # logger.
        self.logger.info('all files have been removed from the system')
