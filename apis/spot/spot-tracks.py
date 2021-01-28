from math import ceil
from time import time, sleep
from os.path import join as os_path_join, split as os_split_path, realpath

from apis.spot.base.spot_api import SpotifyApi
from apis.spot.base.spot_endpoints import SpotifyTrackEndpoints

from utils.list import chunks


class SpotifyTracksApi(SpotifyApi):

    def __init__(self, output_path: list):

        # initialise superclass.
        super().__init__()

        # paths.
        self._base_path: str = os_split_path(os_split_path(os_split_path(realpath(__file__))[0])[0])[0]
        self._output_path: str = os_path_join(*output_path)

    def _clear_containers(self):

        self.data_container.clear()

    def download_track(self, track_id: str):

        # assert input
        if not isinstance(track_id, str) or len(track_id) == 0:
            self.logger.error(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')
            raise ValueError(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')

        # clear all containers.
        self._clear_containers()

        # download data.
        self._download_single(
            data_id=SpotifyTrackEndpoints.GET_TRACK.name,
            endpoint=SpotifyTrackEndpoints.GET_TRACK.value,
            id=track_id
        )

        # if data was downloaded, save data.
        if len(self.data_container) > 0:
            # save to .json file.
            self._save_json_to_file(
                output_path=[self._base_path, self._output_path, 'spot-track-data', track_id],
                data=self.data_container.pop()
            )
            # logger.
            self.logger.info('track download completed')
        else:
            self.logger.error('no data was downloaded')

    def download_several_tracks(self, track_ids: list):

        # assert input
        if not isinstance(track_ids, list) or len(track_ids) == 0:
            self.logger.error(f'expected a valid non empty list, but found {track_ids} of type {type(track_ids)}')
            raise ValueError(f'expected a valid non empty list, but found {track_ids} of type {type(track_ids)}')

        # clear all containers.
        self._clear_containers()

        # download data.
        n_chunks: int = ceil(len(track_ids) / 50)
        for index, chunk in enumerate(chunks(track_ids, 50)):
            # logger.
            self.logger.info(f'iterating chunk {index + 1} of {n_chunks}')
            # get data.
            self._download_multiple(
                data_id=SpotifyTrackEndpoints.GET_SEVERAL_TRACKS.name,
                endpoint=SpotifyTrackEndpoints.GET_SEVERAL_TRACKS.value,
                ids=chunk
            )
            # avoid flooding.
            sleep(1)

        # if data was downloaded, save data.
        if len(self.data_container) > 0:
            # iterate container.
            time_signature = int(time())
            for index, data in enumerate(self.data_container):
                # save to .json file.
                self._save_json_to_file(
                    output_path=[
                        self._base_path, self._output_path, 'spot-track-data', f'{time_signature}-{index}'
                    ],
                    data=data
                )
            # logger.
            self.logger.info('track download completed')
        else:
            # logger.
            self.logger.error('no data was downloaded')

    def download_track_features(self, track_id: str):

        # assert input
        if not isinstance(track_id, str) or len(track_id) == 0:
            self.logger.error(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')
            raise ValueError(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')

        # clear all containers.
        self._clear_containers()

        # download data.
        self._download_single(
            data_id=SpotifyTrackEndpoints.GET_TRACK_AUDIO_FEATURES.name,
            endpoint=SpotifyTrackEndpoints.GET_TRACK_AUDIO_FEATURES.value,
            id=track_id
        )

        # if data was downloaded, save data.
        if len(self.data_container) > 0:
            # save to .json file.
            self._save_json_to_file(
                output_path=[self._base_path, self._output_path, 'spot-track-audio-features', track_id],
                data=self.data_container.pop()
            )
            # logger.
            self.logger.info("track's audio features download completed")
        else:
            self.logger.error('no data was downloaded')

    def download_several_tracks_features(self, track_ids: list):

        # assert input
        if not isinstance(track_ids, list) or len(track_ids) == 0:
            self.logger.error(f'expected a valid non empty list, but found {track_ids} of type {type(track_ids)}')
            raise ValueError(f'expected a valid non empty list, but found {track_ids} of type {type(track_ids)}')

        # clear all containers.
        self._clear_containers()

        # download data.
        n_chunks: int = ceil(len(track_ids) / 50)
        for index, chunk in enumerate(chunks(track_ids, 50)):
            # logger.
            self.logger.info(f'iterating chunk {index + 1} of {n_chunks}')
            # get data.
            self._download_multiple(
                data_id=SpotifyTrackEndpoints.GET_SEVERAL_TRACKS_AUDIO_FEATURES.name,
                endpoint=SpotifyTrackEndpoints.GET_SEVERAL_TRACKS_AUDIO_FEATURES.value,
                ids=chunk
            )
            # avoid flooding.
            sleep(1)

        # if data was downloaded, save data.
        if len(self.data_container) > 0:
            # iterate container.
            time_signature = int(time())
            for index, data in enumerate(self.data_container):
                # save to .json file.
                self._save_json_to_file(
                    output_path=[
                        self._base_path, self._output_path, 'spot-track-audio-features', f'{time_signature}-{index}'
                    ],
                    data=data
                )
            # logger.
            self.logger.info("tracks' audio features download completed")
        else:
            # logger.
            self.logger.error('no data was downloaded')

    def download_audio_analysis(self, track_id: str):

        # assert input
        if not isinstance(track_id, str) or len(track_id) == 0:
            self.logger.error(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')
            raise ValueError(f'expected a valid non empty string, but found {track_id} of type {type(track_id)}')

        # clear all containers.
        self._clear_containers()

        # download data.
        self._download_single(
            data_id=SpotifyTrackEndpoints.GET_TRACK_AUDIO_ANALYSIS.name,
            endpoint=SpotifyTrackEndpoints.GET_TRACK_AUDIO_ANALYSIS.value,
            id=track_id
        )

        # if data was downloaded, save data.
        if len(self.data_container) > 0:
            # save to .json file.
            self._save_json_to_file(
                output_path=[self._base_path, self._output_path, 'spot-track-audio-analysis', track_id],
                data=self.data_container.pop()
            )
            # logger.
            self.logger.info('track audio analysis data download completed')
        else:
            self.logger.error('no data was downloaded')


if __name__ == '__main__':

    # get track ids.
    import csv
    with open('track_ids.txt', 'r', newline='\r\n', encoding='utf-8') as f:
        track_ids = [line[0] for line in csv.reader(f)]

    # get new instance.
    spot_tracks = SpotifyTracksApi(output_path=['data', 'raw'])
    # # get single track's data.
    # spot_tracks.download_track(track_id='5aAx2yezTd8zXrkmtKl66Z')
    # # get multiple tracks' data.
    # spot_tracks.download_several_tracks(track_ids=track_ids)
    # # get single track's audio features.
    # spot_tracks.download_track_features(track_id='5aAx2yezTd8zXrkmtKl66Z')
    # # get multiple tracks' audio features.
    # spot_tracks.download_several_tracks_features(track_ids=track_ids)
    # get single track's audio analysis
    for track in track_ids:
        spot_tracks.download_audio_analysis(track_id=track)
        sleep(2)
