import os

import backoff
import requests

from datetime import datetime as dt
from time import sleep

from utils.api import Api


class WeeklyChartsDownloader(Api):

    # available regions.
    regions = [
        "global", "gb", "ar", "au", "bg", "br", "ch", "co", "cy", "de", "do", "ee", "fi",
        "gr", "hk", "hu", "ie", "in", "it", "lt", "lv", "my", "nl", "nz", "pe", "pl", "py",
        "ru", "sg", "sv", "tr", "ua", "vn"
    ]

    def __init__(self, system_logger: bool = False):

        # initialise superclass.
        super().__init__()

        # base path.
        self._base_path = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]

        # initialise logger.
        self._init_logger(logger_name='SPOT-WKLY', file_name=f'RUN {dt.now()}', system_logger=system_logger)

    def download_weekly_charts(self, weeks: list, region: str = 'global'):

        # assert weeks input.
        if not isinstance(weeks, list):
            self.logger.error(f'expected a list argument, not {type(weeks)}')
            raise ValueError(f'expected a list argument, not {type(weeks)}')
        elif len(weeks) == 0:
            self.logger.error('empty weeks list was supplied, at least one week is required')
            raise ValueError('empty weeks list was supplied, at least one week is required')

        # assert region input
        if not isinstance(region, str) or region in (None, ''):
            self.logger.error('region must be a valid non empty string')
            raise ValueError('region must be a valid non empty string')
        elif region not in self.regions:
            self.logger.error(f'the region value specified {region} is not allowed')
            raise ValueError(f'the region value specified {region} is not allowed')

        # logger.
        self.logger.info(f'initialising data request for {len(weeks)}')

        # iterate and download weeks.
        for index, week in enumerate(weeks):
            # logger.
            self.logger.info(f'iterating week {week} - {index} of {len(weeks)}')
            # perform data download.
            self._download_weekly_charts(week=week, region=region)
            # avoid flooding.
            sleep(1)

        # logger.
        self.logger.info(f'weekly data download completed')

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=5)
    def _download_weekly_charts(self, week: str, region: str):

        # perform api
        r = requests.get(
            url=f'https://spotifycharts.com/regional/{region}/weekly/{week}/download',
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US;q=0.5',
                'referer': f'https://spotifycharts.com/regional/ar/weekly/{week}',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
            }
        )

        # check response.
        if not r.ok:
            # log error.
            self.logger.error(f'endpoint responded with status code {r.status_code}')
            return

        # save data to filesystem.
        super()._save_text_to_file(
            output_path=[self._base_path, 'data', 'raw', 'weekly-charts', week],
            data=r.text,
            extension='csv'
        )


if __name__ == '__main__':
    # weeks to download
    weeks = [
        "2021-01-15--2021-01-22",
        "2021-01-01--2021-01-08",
        "2020-12-18--2020-12-25",
        "2020-12-04--2020-12-11",
        "2020-11-20--2020-11-27",
        "2020-11-06--2020-11-13",
        "2020-10-23--2020-10-30",
        "2020-10-09--2020-10-16",
        "2020-09-25--2020-10-02",
        "2020-09-11--2020-09-18",
        "2020-08-28--2020-09-04",
        "2020-08-14--2020-08-21",
        "2020-07-31--2020-08-07",
        "2020-07-17--2020-07-24",
        "2020-07-03--2020-07-10",
        "2020-06-19--2020-06-26",
        "2020-06-05--2020-06-12",
        "2020-05-22--2020-05-29",
        "2020-05-08--2020-05-15",
        "2020-04-24--2020-05-01",
        "2020-04-10--2020-04-17",
        "2020-03-27--2020-04-03",
        "2020-03-13--2020-03-20",
        "2020-02-28--2020-03-06",
        "2020-02-14--2020-02-21",
        "2020-01-31--2020-02-07",
        "2020-01-17--2020-01-24",
        "2020-01-03--2020-01-10",
        "2019-12-20--2019-12-27",
        "2019-12-06--2019-12-13",
        "2019-11-22--2019-11-29",
        "2019-11-08--2019-11-15",
        "2019-10-25--2019-11-01",
        "2019-10-11--2019-10-18",
        "2019-09-27--2019-10-04",
        "2019-09-13--2019-09-20",
        "2019-08-30--2019-09-06",
        "2019-08-16--2019-08-23",
        "2019-08-02--2019-08-09",
        "2019-07-19--2019-07-26",
        "2019-07-05--2019-07-12",
        "2019-06-21--2019-06-28",
        "2019-06-07--2019-06-14",
        "2019-05-24--2019-05-31",
        "2019-05-10--2019-05-17",
        "2019-04-26--2019-05-03",
        "2019-04-12--2019-04-19",
        "2019-03-29--2019-04-05",
        "2019-03-15--2019-03-22",
        "2019-03-01--2019-03-08",
        "2019-02-15--2019-02-22",
        "2019-02-01--2019-02-08",
        "2019-01-18--2019-01-25",
        "2019-01-04--2019-01-11",
        "2018-12-21--2018-12-28",
        "2018-12-07--2018-12-14",
        "2018-11-23--2018-11-30",
        "2018-11-09--2018-11-16",
        "2018-10-26--2018-11-02",
        "2018-10-12--2018-10-19",
        "2018-09-28--2018-10-05",
        "2018-09-14--2018-09-21",
        "2018-08-31--2018-09-07",
        "2018-08-17--2018-08-24",
        "2018-08-03--2018-08-10",
        "2018-07-20--2018-07-27",
        "2018-07-06--2018-07-13",
        "2018-06-22--2018-06-29",
        "2018-06-08--2018-06-15",
        "2018-05-25--2018-06-01",
        "2018-05-11--2018-05-18",
        "2018-04-27--2018-05-04",
        "2018-04-13--2018-04-20",
        "2018-03-30--2018-04-06",
        "2018-03-16--2018-03-23",
        "2018-03-02--2018-03-09",
        "2018-02-16--2018-02-23",
        "2018-02-02--2018-02-09",
        "2018-01-19--2018-01-26",
        "2018-01-05--2018-01-12",
        "2017-12-22--2017-12-29",
        "2017-12-08--2017-12-15",
        "2017-11-24--2017-12-01",
        "2017-11-10--2017-11-17",
        "2017-10-27--2017-11-03",
        "2017-10-13--2017-10-20",
        "2017-09-29--2017-10-06",
        "2017-09-15--2017-09-22",
        "2017-09-01--2017-09-08",
        "2017-08-18--2017-08-25",
        "2017-08-04--2017-08-11",
        "2017-07-21--2017-07-28",
        "2017-07-07--2017-07-14",
        "2017-06-23--2017-06-30",
        "2017-06-09--2017-06-16",
        "2017-05-12--2017-05-19",
        "2017-04-28--2017-05-05",
        "2017-04-14--2017-04-21",
        "2017-03-31--2017-04-07",
        "2017-03-17--2017-03-24",
        "2017-03-03--2017-03-10",
        "2017-02-17--2017-02-24",
        "2017-02-03--2017-02-10",
        "2017-01-20--2017-01-27",
        "2017-01-06--2017-01-13",
        "2016-12-23--2016-12-30"
    ]
    # perform download.
    wd = WeeklyChartsDownloader(system_logger=False)
    wd.download_weekly_charts(weeks=weeks[0:2])
