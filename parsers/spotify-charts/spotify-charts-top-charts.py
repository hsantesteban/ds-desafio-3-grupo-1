import csv

from os.path import join as os_path_join, splitext, split as os_split_path, realpath

from utils.parser import Parser


class SpotifyChartsParser(Parser):

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

    def parse_weekly_files(self, input_files_path: list, output_files_path: list, delimiter: str = ',',
                           limit: int = 99999):
        """
        Parses weekly SPOT charts CSV files.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param delimiter: str - delimiter to use while parsing CSVs, defaults to ','.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising weekly charts files parsing')

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
        super()._read_files(path=os_path_join(self._base_path, *input_files_path), allowed_extensions=('.csv',),
                            max_files=limit)

        # iterate and parse files container.
        files: int = len(self._raw_data_container)
        for index, file in enumerate(self._raw_data_container):
            # logger.
            self.logger.info(f'iterating file {self._files_container[index]} - {index + 1} of {files}')
            # summon parser.
            self._parse_weekly_files(
                output_path=output_files_path, raw_data=file, file_name=self._files_container[index],
                delimiter=delimiter
            )

    def _parse_weekly_files(self, output_path: list, raw_data: str, file_name: str, delimiter: str = ','):

        # original columns: "Position", "Track Name", "Artist", "Streams", "URL"
        column_region: str = os_split_path(splitext(file_name)[0])[1].split('_')[0]
        column_week: str = os_split_path(splitext(file_name)[0])[1].split('_')[1]
        column_date_from: str = column_week.split('--')[0]
        column_date_to: str = column_week.split('--')[1]

        # iterate rows in data.
        with open(raw_data, encoding='utf-8') as csv_file:
            # read csv file.
            csv_reader = csv.reader(csv_file, delimiter=delimiter)
            # skip headers and texts.
            next(csv_reader, None)
            next(csv_reader, None)
            # iterate contents.
            for index, row in enumerate(csv_reader):
                # parse data into sql format.
                column_track_id: str = row[4].split('/').pop()
                column_track_name: str = row[1]
                column_track_artist: str = row[2]
                column_track_position: str = row[0]
                column_track_streams: str = row[3]
                column_track_url: str = row[4]
                # append data to container.
                self._data_container.append(
                    tuple([
                        column_region, column_week, column_date_from, column_date_to, column_track_id,
                        column_track_name, column_track_artist, column_track_position, column_track_streams,
                        column_track_url
                    ])
                )

        # logger.
        self.logger.info(f'weekly file {file_name} parsed, now saving data to a new file')

        # save file to parsed folder.
        fields = ['region', 'week', 'date_from', 'date_to', 'track_id', 'track_name', 'artist',
                  'track_position', 'track_streams', 'track_url']
        parsed_file_path = os_path_join(self._base_path, *output_path, f'{column_region}_{column_week}.csv')
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
            self.logger.info(f'weekly file {file_name} parsed data saved')

    def consolidate_weekly_files(self, input_files_path: list, output_files_path: list, output_file_name: str,
                                 delimiter: str = ',', limit: int = 99999):
        """
        Consolidates parsed weekly SPOT charts CSV files into a single CSV file.
        :param input_files_path: str - inner project path from which to draw files.
        :param output_files_path: str - inner project path from which to store parsed files.
        :param output_file_name: str - name of the consolidated CSV file that will be created.
        :param delimiter: str - delimiter to use while parsing CSVs, defaults to ','.
        :param limit: int - max files to parse.
        :return:
        """

        # logger.
        self.logger.info('initialising weekly charts files consolidation')

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
        self._consolidate_weekly_files(
            output_path=output_files_path, file_name=output_file_name, delimiter=delimiter
        )

    def _consolidate_weekly_files(self, output_path: list, file_name: str, delimiter: str):

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


if __name__ == '__main__':

    # instantiate a new spotify charts parser.
    wcp = SpotifyChartsParser()

    # trigger weekly files parsing.
    # wcp.parse_weekly_files(
    #     input_files_path=['data', 'raw', 'spotify-charts-weekly-top-charts'],
    #     output_files_path=['data', 'parsed', 'spotify-charts-weekly-top-charts'],
    #     delimiter=',',
    #     # limit=2
    # )

    # trigger weekly files consolidating.
    wcp.consolidate_weekly_files(
        input_files_path=['data', 'parsed', 'spotify-charts-weekly-top-charts'],
        output_files_path=['data', 'consolidated', 'spotify-charts-weekly-top-charts'],
        output_file_name='consolidated_weekly_charts',
        delimiter=',',
        # limit=10
    )
