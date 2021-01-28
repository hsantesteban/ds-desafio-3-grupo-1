import requests
import backoff

from utils.api import Api


class SpotifyApi(Api):

    def __init__(self):

        # initialise superclass.
        super().__init__()

        # in memory logger.
        self._init_logger(logger_name='SPOT', file_name='', system_logger=False)

        # get new access token.
        self.access_token: str = self._refresh_access_token()

        # data container.
        self.data_container = list()

    def _refresh_access_token(self) -> str:

        # assemble data for request.
        api_url = 'https://accounts.spotify.com/api/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.environment.get('SPOT_REFRESH_TOKEN'),
            'client_id': self.environment.get('SPOT_CLIENT_ID'),
            'client_secret': self.environment.get('SPOT_CLIENT_SECRET')
        }

        # perform request.
        response = requests.post(url=api_url, headers=headers, data=data)

        # update credentials on success.
        if response.ok:
            # parse data
            json_response = response.json()
            # extract data
            access_token = json_response.get('access_token', None)
            # set token.
            self.access_token = access_token

            return access_token

        # raise Error
        raise ValueError('refresh token could not be generated')

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=5)
    def _download_multiple(self, data_id: str, endpoint: str, ids: list, retries: int = 0):

        # check max retries.
        if retries == 5:
            return

        # logger.
        self.logger.info(f'performing data request for endpoint: {endpoint} for ids {" ".join(ids)}')

        # perform request.
        r = requests.get(
            url=endpoint,
            headers={
                'Authorization': f'Bearer {self.access_token}'
            },
            params={
                'ids': ','.join(ids)
            }
        )

        # evaluate result.
        if r.ok:
            self._append_data_to_container(
                response=r,
                reference={'data_id': data_id, 'endpoint': endpoint, 'ids': ids}
            )
        elif r.status_code == 401:
            self._refresh_access_token()
            self._download_multiple(data_id=data_id, endpoint=endpoint, ids=ids, retries=retries+1)
        else:
            raise requests.exceptions.RequestException(f'endpoint responded with code {r.status_code}')

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=5)
    def _download_single(self, data_id: str, endpoint: str, id: str, retries: int = 0):

        # check max retries.
        if retries == 5:
            return

        # logger.
        self.logger.info(f'performing data request for endpoint: {endpoint} using id {id}')

        # perform request.
        r = requests.get(
            url=endpoint.format(id=id),
            headers={
                'Authorization': f'Bearer {self.access_token}'
            }
        )

        # evaluate result.
        if r.ok:
            self._append_data_to_container(
                response=r,
                reference={'data_id': data_id, 'endpoint': endpoint, 'id': id}
            )
        elif r.status_code == 401:
            self._refresh_access_token()
            self._download_single(data_id=data_id, endpoint=endpoint, id=id, retries=retries+1)
        elif r.status_code == 404:
            self.logger.error(f'endpoint responded with status code: {r.status_code}')
            return
        else:
            raise requests.exceptions.RequestException(f'endpoint responded with code {r.status_code}')

    def _append_data_to_container(self, response: requests.Response, reference: dict):

        # attempt to perform JSON parsing.
        try:
            raw_data = response.json()
        except:
            # log error.
            self.logger.error(f'JSONDecodeError while parsing response')
            # return to prevent further processing.
            return

        # logger.
        self.logger.info('appending data to container')

        # append data to container.
        self.data_container.append({**reference, 'raw_data': raw_data})
