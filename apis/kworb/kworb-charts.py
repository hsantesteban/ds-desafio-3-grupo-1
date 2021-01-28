import backoff
import requests

from os.path import join as os_path_join, split as os_split_path, realpath

from utils.api import Api


class KworbChartsApi(Api):

    # https://kworb.net/spotify/artists.html

    def __init__(self, output_path: list):

        # initialise superclass.
        super().__init__()

        # initialise logger.
        self._init_logger(logger_name='KWORB', file_name='', system_logger=False)

        # paths.
        self._base_path: str = os_split_path(os_split_path(os_split_path(realpath(__file__))[0])[0])[0]
        self._output_path: str = os_path_join(*output_path)

    def download_track_charts_history(self, track_id: str):

        # logger.
        self.logger.info(f'initialising retrieval of charts history for track: {track_id}')

        # check arguments.
        if not isinstance(track_id, str) or len(track_id) == 0:
            self.logger.error(f'a valid non empty string was expected, but received {track_id} {type(track_id)}')
            raise ValueError(f'a valid non empty string was expected, but received {track_id} {type(track_id)}')

        # fetch data.
        try:
            data: str = self._download_data(url=f'https://kworb.net/spotify/track/{track_id}.html')
        except requests.exceptions.RequestException:
            self.logger.error(f'aborting download for id {track_id}')
            return

        # save data to file system.
        self._save_text_to_file(
            output_path=[self._base_path, self._output_path, 'kworb-charts-track', track_id],
            data=data,
            extension='html'
        )

    def download_artists_charts_history(self, artist_id: str):

        # logger.
        self.logger.info(f'initialising retrieval of charts history for artist: {artist_id}')

        # check arguments.
        if not isinstance(artist_id, str) or len(artist_id) == 0:
            self.logger.error(f'a valid non empty string was expected, but received {artist_id} {type(artist_id)}')
            raise ValueError(f'a valid non empty string was expected, but received {artist_id} {type(artist_id)}')

        # fetch data.
        data: str = self._download_data(url=f'https://kworb.net/spotify/artist/{artist_id}.html')

        # save data to file system.
        self._save_text_to_file(
            output_path=[self._base_path, self._output_path, 'kworb-charts-artist', artist_id],
            data=data,
            extension='html'
        )

    def download_global_charts_history(self, interval: str = 'weekly', region: str = 'global'):

        # logger.
        self.logger.info(f'initialising retrieval of charts history for region {region} and interval {interval}')

        # fetch data.
        data: str = self._download_data(url=f'https://kworb.net/spotify/country/{region}_{interval}_totals.html')

        # save data to file system.
        self._save_text_to_file(
            output_path=[self._base_path, self._output_path, 'kworb-charts-region', f'{region}-{interval}'],
            data=data,
            extension='html'
        )

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=5)
    def _download_data(self, url: str) -> str:

        # perform request.
        r = requests.get(
            url=url,
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9,es-419;q=0.8,es;q=0.7,es-ES;q=0.6,en-GB;q=0.5,pt;q=0.4,pt-BR;q=0.3',
                'referer': 'https://kworb.net/spotify/country/global_weekly_totals.html',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            }
        )

        # check response.
        if not r.ok:
            # logger.
            self.logger.error(f'endpoint responded with status code {r.status_code}')
            raise requests.exceptions.RequestException(f'endpoint responded with status code {r.status_code}')

        # return html text.
        return r.text


if __name__ == '__main__':

    import csv

    # available regions.
    regions = [
        "global", "us", "gb", "ad", "ar", "au", "at", "be", "bo", "br", "bg", "ca", "cl", "co", "cr", "cy", "cz",
        "dk", "do", "ec", "sv", "ee", "fi", "fr", "de", "gr", "gt", "hn", "hk", "hu", "is", "id", "in", "ie", "il",
        "it", "jp", "lv", "lt", "lu", "my", "mt", "mx", "nl", "nz", "ni", "no", "pa", "py", "pe", "ph", "pl", "pt",
        "ro", "sg", "sk", "es", "se", "ch", "tw", "th", "tr", "uy", "vn"
    ]

    # available tracks.
    with open('track_ids.txt', 'r', newline='\r\n', encoding='utf-8') as f:
        tracks = [line[0] for line in csv.reader(f)]

    # available artists.
    artists = []

    # instantiate a new downloader object.
    k_charts = KworbChartsApi(output_path=['data', 'raw'])

    # download track's charts.
    for track in tracks:
        k_charts.download_track_charts_history(track_id=track)

    # download global charts.
    # for region in regions:
    #     k_charts.download_global_charts_history(interval='weekly', region=region)

    # # download artist's charts.
    # for artist in artists:
    #     k_charts.download_artists_charts_history(artist_id='')
