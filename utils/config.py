import io

from os import path


class Config:

    # defaults.
    env: dict = {
        'SPOT_REFRESH_TOKEN': '',
        'SPOT_CLIENT_ID': '',
        'SPOT_CLIENT_SECRET': ''
    }

    # path to .env file.
    env_filename = '../.env'

    @classmethod
    def static_init(cls):
        """
        Static initializer.
        :return:
        """

        # check file existence.
        if path.isfile(path.join(path.dirname(path.realpath(__file__)), Config.env_filename)):
            # load file contents to env class attribute.
            cls._load_env(path.join(path.dirname(path.realpath(__file__)), Config.env_filename))
            return cls.env
        else:
            ValueError('could not read environment file (.env), please create if missing.')

    @classmethod
    def _load_env(cls, env_path: str) -> None:
        """
        Load environment variables from .env
        :param env_path:
        :return:
        """

        cls.env = cls._read_environment_file(env_path=env_path)

    @staticmethod
    def _read_environment_file(env_path: str) -> dict:
        """
        Reads environment file from filesystem and assigns its contents to a local dictionary.
        :param path: path towards the .evn file.
        :return: dict.
        """

        # temporary dict.
        tmp_env = dict()

        # open file.
        with io.open(env_path) as stream:
            # iterate contents.
            for line in stream:
                # skip comments.
                if line[0] == '#':
                    continue
                # split equalities.
                parts = line.split(sep='=', maxsplit=1)
                # update temporary dictionary.
                tmp_env[parts[0]] = parts[1].strip().replace("'", "").replace("\\", "")

            return tmp_env.copy()
