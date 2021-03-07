import json
import os
import pathlib

import google.auth
import google.auth.transport.requests
import requests

DATA_PATH = f'{pathlib.Path(__file__).absolute().parent}/data'


def get_auth_token(credential_path):
    """
    Returns Goggle OAuth 2.0 access token
    """
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(
        scopes=['https://www.googleapis.com/auth/bigquery'])

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    # print(credentials.valid)  # prints False
    credentials.refresh(auth_req)  # refresh token
    # cehck for valid credentials
    # print(credentials.valid)  # prints True
    return credentials.token  # prints toke


def get(url, credential_path=DATA_PATH):
    """
    Makes a REST GET call to the given url and returns response
    """
    params = {'key': 'AIzaSyDjMXUNSD1-1_oxT2hk5Ow4uaPg0IeCBck'}
    bearer = get_auth_token(credential_path)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8',
               'Authorization': 'Bearer ' + bearer}
    # print(url, headers)
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return json.loads(response.content)


def main():
    print(get(
        'https://bigquery.googleapis.com/bigquery/v2/projects/bigquery-public-data/datasets?all=true',
        f'{DATA_PATH}/amit-pradhan-compute-f61ddefef705.json'))


if __name__ == '__main__':
    main()
