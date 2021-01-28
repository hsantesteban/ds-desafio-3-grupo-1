import csv

from apis.spot.base.spot_endpoints import SpotifyTrackEndpoints

from utils.parser import Parser

from os.path import join as os_path_join, splitext, split as os_split_path, realpath


class SpotTrackParser(Parser):

    def __init__(self):
        # initialise superclass.
        super().__init__()

        # base path.
        self._base_path = os_split_path(os_split_path(os_split_path(realpath(__file__))[0])[0])[0]

        # initialise data container.
        self._data_container: list = list()

    def _clear_containers(self):
        # clear all containers.
        super()._clear_containers()
        self._data_container.clear()

    def parse_audio_features_files(self, input_files_path: list, output_files_path: list, limit=999999):
        """
        Parses SPOT audio features JSON files.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising audio features files parsing')

        # clear all containers.
        self._clear_containers()

        # assert files_path.
        if not isinstance(input_files_path, list):
            self.logger.error(f'expected a list argument, not {type(input_files_path)}')
            raise ValueError(f'expected a list argument, not {type(input_files_path)}')
        elif len(input_files_path) == 0:
            self.logger.error('empty input_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty input_files_path list was supplied, at least one valid path is required')
        if not isinstance(output_files_path, list):
            self.logger.error(f'expected a list argument, not {type(output_files_path)}')
            raise ValueError(f'expected a list argument, not {type(output_files_path)}')
        elif len(output_files_path) == 0:
            self.logger.error('empty output_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty output_files_path list was supplied, at least one valid path is required')

        # read files from container folder.
        super()._read_files(path=os_path_join(self._base_path, *input_files_path), allowed_extensions=('.json',),
                            max_files=limit)

        # iterate and parse files container.
        files: int = len(self._raw_data_container)
        for index, file in enumerate(self._raw_data_container):
            # logger.
            self.logger.info(f'iterating file {self._files_container[index]} - {index + 1} of {files}')
            # summon parser.
            self._parse_audio_features_files(
                output_path=output_files_path, raw_data=file, file_name=self._files_container[index]
            )

    def _parse_audio_features_files(self, output_path: list, raw_data: dict, file_name: str):

        # fetch filename.
        parsed_name: str = os_split_path(splitext(file_name)[0])[1]

        # parse individual files.
        if raw_data.get('data_id') == SpotifyTrackEndpoints.GET_TRACK_AUDIO_FEATURES.name:
            self._parse_audio_features_file(data=raw_data.get('raw_data', {}))

        # parse container files.
        if raw_data.get('data_id') == SpotifyTrackEndpoints.GET_SEVERAL_TRACKS_AUDIO_FEATURES.name:
            for data in raw_data.get('raw_data', {}).get('audio_features', {}):
                self._parse_audio_features_file(data=data)

        # logger.
        self.logger.info(f'audio features file {file_name} parsed, now saving data to a new file')

        # save file to parsed folder.
        fields = [
            'track_id', 'duration_ms', 'time_signature', 'tempo', 'key', 'mode', 'valence', 'liveness',
            'instrumentalness',
            'acousticness', 'speechiness', 'loudness', 'energy', 'danceability'
        ]
        parsed_file_path = os_path_join(self._base_path, *output_path, f'{parsed_name}.csv')
        with open(parsed_file_path, 'w', newline='', encoding='utf-8') as f:
            # instantiate a new csv writer.
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            # write headers.
            writer.writerow(fields)
            # write contents.
            writer.writerows(self._data_container)
            # clear data container.
            self._data_container.clear()
            # logger.
            self.logger.info(f'audio features file {file_name} parsed data saved')

    def _parse_audio_features_file(self, data: dict):

        # extract data.
        column_id: str = data.get('id')
        column_duration_ms: int = data.get('duration_ms')
        column_time_signature: int = data.get('time_signature')
        column_tempo: float = data.get('tempo')
        column_key: int = data.get('key')
        column_mode: int = data.get('mode')
        column_valence: float = data.get('valence')
        column_liveness: float = data.get('liveness')
        column_instrumentalness: float = data.get('instrumentalness')
        column_acousticness: float = data.get('acousticness')
        column_speechiness: float = data.get('speechiness')
        column_loudness: int = data.get('loudness')
        column_energy: float = data.get('energy')
        column_danceability: float = data.get('danceability')

        # append data to data container.
        self._data_container.append(
            tuple([
                column_id, column_duration_ms, column_time_signature, column_tempo, column_key, column_mode,
                column_valence, column_liveness, column_instrumentalness, column_acousticness, column_speechiness,
                column_loudness, column_energy, column_danceability
            ])
        )

        # logger.
        self.logger.info(f'successfully parsed track id {column_id}')

    def consolidate_audio_files_files(self, input_files_path: list, output_files_path: list, output_file_name: str,
                                      delimiter: str = ',', limit: int = 99999):
        """
        Consolidates parsed audio features CSV files into a single CSV file.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param output_file_name: str - name of the consolidated CSV file that will be created.
        :param delimiter: str - delimiter to use while parsing CSVs, defaults to ','.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising audio features files consolidation')

        # clear all containers.
        self._clear_containers()

        # input files path.
        if not isinstance(input_files_path, list):
            self.logger.error(f'expected a list argument, not {type(input_files_path)}')
            raise ValueError(f'expected a list argument, not {type(input_files_path)}')
        elif len(input_files_path) == 0:
            self.logger.error('empty input_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty input_files_path list was supplied, at least one valid path is required')
        # output files path.
        if not isinstance(output_files_path, list):
            self.logger.error(f'expected a list argument, not {type(output_files_path)}')
            raise ValueError(f'expected a list argument, not {type(output_files_path)}')
        elif len(output_files_path) == 0:
            self.logger.error('empty output_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty output_files_path list was supplied, at least one valid path is required')
        # output file name.
        if not isinstance(output_file_name, str) or len(output_file_name) == 0:
            self.logger.error(f'expected a non empty string argument, not {type(output_file_name)}')
            raise ValueError(f'expected a non empty string argument, not {type(output_file_name)}')

        # read files from container folder.
        super()._read_files(path=os_path_join(self._base_path, *input_files_path), allowed_extensions=('.csv',),
                            max_files=limit)

        # consolidate files.
        self._consolidate_files(
            output_path=output_files_path, file_name=output_file_name, delimiter=delimiter
        )

    def _consolidate_files(self, output_path: list, file_name: str, delimiter: str):

        # open consolidated file.
        final_output_path = os_path_join(self._base_path, *output_path, f'{file_name}.csv')
        with open(final_output_path, 'w', newline='', encoding='utf-8') as output_csv:

            # instantiate a new csv writer.
            writer = csv.writer(output_csv, quoting=csv.QUOTE_ALL)

            # iterate and parse files container.
            files: int = len(self._raw_data_container)
            for index, file in enumerate(self._raw_data_container):
                # logger.
                self.logger.info(f'iterating file {self._files_container[index]} - {index + 1} of {files}')
                # open input csv.
                with open(file, 'r', encoding='utf-8') as input_file:
                    # read input csv file.
                    csv_reader = csv.reader(input_file, delimiter=delimiter)
                    # skip header, except for the first file.
                    if index != 0:
                        next(csv_reader, None)
                    # write lines to output file.
                    for row in csv_reader:
                        writer.writerow(row)
                    # logger.
                    self.logger.info(f'file {file} consolidated')

        # logger.
        self.logger.info('all files have been consolidated')

    def parse_track_data_files(self, input_files_path: list, output_files_path: list, limit=99999):
        """
        Parses SPOT track data JSON files.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising track data files parsing')

        # clear all containers.
        self._clear_containers()

        # assert files_path.
        if not isinstance(input_files_path, list):
            self.logger.error(f'expected a list argument, not {type(input_files_path)}')
            raise ValueError(f'expected a list argument, not {type(input_files_path)}')
        elif len(input_files_path) == 0:
            self.logger.error('empty input_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty input_files_path list was supplied, at least one valid path is required')
        if not isinstance(output_files_path, list):
            self.logger.error(f'expected a list argument, not {type(output_files_path)}')
            raise ValueError(f'expected a list argument, not {type(output_files_path)}')
        elif len(output_files_path) == 0:
            self.logger.error('empty output_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty output_files_path list was supplied, at least one valid path is required')

        # read files from container folder.
        super()._read_files(path=os_path_join(self._base_path, *input_files_path), allowed_extensions=('.json',),
                            max_files=limit)

        # iterate and parse files container.
        files: int = len(self._raw_data_container)
        for index, file in enumerate(self._raw_data_container):
            # logger.
            self.logger.info(f'iterating file {self._files_container[index]} - {index + 1} of {files}')
            # summon parser.
            self._parse_track_data_files(
                output_path=output_files_path, raw_data=file, file_name=self._files_container[index]
            )

    def _parse_track_data_files(self, output_path: list, raw_data: dict, file_name: str):

        # fetch filename.
        parsed_name: str = os_split_path(splitext(file_name)[0])[1]

        # parse individual files.
        if raw_data.get('data_id') == SpotifyTrackEndpoints.GET_TRACK.name:
            self._parse_audio_features_file(data=raw_data.get('raw_data', {}))

        # parse container files.
        if raw_data.get('data_id') == SpotifyTrackEndpoints.GET_SEVERAL_TRACKS.name:
            for data in raw_data.get('raw_data', {}).get('tracks', {}):
                self._parse_track_data_file(data=data)

        # logger.
        self.logger.info(f'track data file {file_name} parsed, now saving data to a new file')

        # save file to parsed folder.
        fields = [
            'track_id', 'track_name', 'type', 'popularity', 'duration_ms', 'is_explicit', 'is_local', 'artist_id',
            'artist_name', 'artist_type', 'feat_artists_id', 'feat_artists_name', 'album_id', 'album_name',
            'album_type', 'album_release_date', 'album_total_tracks'
        ]
        parsed_file_path = os_path_join(self._base_path, *output_path, f'{parsed_name}.csv')
        with open(parsed_file_path, 'w', newline='', encoding='utf-8') as f:
            # instantiate a new csv writer.
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            # write headers.
            writer.writerow(fields)
            # write contents.
            writer.writerows(self._data_container)
            # clear data container.
            self._data_container.clear()
            # logger.
            self.logger.info(f'track data file {file_name} parsed data saved')

    def _parse_track_data_file(self, data: dict):

        # parse main node.
        column_track_id: str = data.get('id')
        column_track_name: str = data.get('name')
        column_type: str = data.get('type')
        column_popularity: int = data.get('popularity')
        column_duration_ms: int = data.get('duration_ms')
        column_is_explicit: bool = data.get('explicit')
        column_is_local: bool = data.get('is_local')
        # parse artists sub node.
        tmp_track_artists = data.get('artists', [])
        column_artist_id: str = tmp_track_artists[0].get('id')
        column_artist_name: str = tmp_track_artists[0].get('name')
        column_artist_type: str = tmp_track_artists[0].get('type')
        column_feat_artists_id: str = "-".join(map(lambda x: x.get('id'), tmp_track_artists[1:])) \
            if len(tmp_track_artists) > 1 else None
        column_feat_artists_name: str = "-".join(map(lambda x: x.get('name'), tmp_track_artists[1:])) \
            if len(tmp_track_artists) > 1 else None
        # parse album sub node.
        column_album_id: str = data.get('album', {}).get('id')
        column_album_name: str = data.get('album', {}).get('name')
        column_album_type: str = data.get('album', {}).get('album_type')
        column_album_release_date: str = data.get('album', {}).get('release_date')
        column_album_total_tracks: int = data.get('album', {}).get('total_tracks')

        # append data to data container.
        self._data_container.append(
            tuple([
                column_track_id, column_track_name, column_type, column_popularity, column_duration_ms,
                column_is_explicit, column_is_local, column_artist_id, column_artist_name, column_artist_type,
                column_feat_artists_id, column_feat_artists_name, column_album_id, column_album_name,
                column_album_type, column_album_release_date, column_album_total_tracks
            ])
        )

        # logger.
        self.logger.info(f'successfully parsed track id {column_track_id}')

    def consolidate_track_data_files(self, input_files_path: list, output_files_path: list, output_file_name: str,
                                     delimiter: str = ',', limit: int = 99999):
        """
        Consolidates parsed track data CSV files into a single CSV file.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param output_file_name: str - name of the consolidated CSV file that will be created.
        :param delimiter: str - delimiter to use while parsing CSVs, defaults to ','.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising track data files consolidation')

        # clear all containers.
        self._clear_containers()

        # input files path.
        if not isinstance(input_files_path, list):
            self.logger.error(f'expected a list argument, not {type(input_files_path)}')
            raise ValueError(f'expected a list argument, not {type(input_files_path)}')
        elif len(input_files_path) == 0:
            self.logger.error('empty input_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty input_files_path list was supplied, at least one valid path is required')
        # output files path.
        if not isinstance(output_files_path, list):
            self.logger.error(f'expected a list argument, not {type(output_files_path)}')
            raise ValueError(f'expected a list argument, not {type(output_files_path)}')
        elif len(output_files_path) == 0:
            self.logger.error('empty output_files_path list was supplied, at least one valid path is required')
            raise ValueError('empty output_files_path list was supplied, at least one valid path is required')
        # output file name.
        if not isinstance(output_file_name, str) or len(output_file_name) == 0:
            self.logger.error(f'expected a non empty string argument, not {type(output_file_name)}')
            raise ValueError(f'expected a non empty string argument, not {type(output_file_name)}')

        # read files from container folder.
        super()._read_files(path=os_path_join(self._base_path, *input_files_path), allowed_extensions=('.csv',),
                            max_files=limit)

        # consolidate files.
        self._consolidate_files(
            output_path=output_files_path, file_name=output_file_name, delimiter=delimiter
        )


if __name__ == '__main__':
    # create new track parser object.
    stp = SpotTrackParser()

    # parse raw json files into CSV files.
    # stp.parse_audio_features_files(
    #     input_files_path=['data', 'raw', 'spot-track-audio-features'],
    #     output_files_path=['data', 'parsed', 'spot-track-audio-features'],
    #     # limit=2
    # )

    # consolidate parsed CSV files into a single CSV.
    # stp.consolidate_audio_files_files(
    #     input_files_path=['data', 'parsed', 'spot-track-audio-features'],
    #     output_files_path=['data', 'consolidated', 'spot-track-audio-features'],
    #     output_file_name='consolidated_audio_features',
    #     # limit=10
    # )

    # parse raw json files into CSV files.
    # stp.parse_track_data_files(
    #     input_files_path=['data', 'raw', 'spot-track-data'],
    #     output_files_path=['data', 'parsed', 'spot-track-data'],
    #     # limit=2
    # )

    # consolidate parsed CSV files into a single CSV.
    stp.consolidate_track_data_files(
        input_files_path=['data', 'parsed', 'spot-track-data'],
        output_files_path=['data', 'consolidated', 'spot-track-data'],
        output_file_name='consolidated_track_data',
        # limit=10
    )
