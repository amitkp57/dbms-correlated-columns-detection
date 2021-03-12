import json
import os
import pathlib

import google.auth
import google.auth.transport.requests
import requests


# Util methods for making REST calls
def get_auth_token():
    """
    Returns Goggle OAuth 2.0 access token
    """
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


def get(url):
    """
    Makes a REST GET call to the given url and returns response
    """
    params = {'key': 'AIzaSyCIkzCe8fd_8roPjePi_t_kKVBX-aOCwYM'}
    bearer = get_auth_token()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8',
               'Authorization': 'Bearer ' + bearer}
    # print(url, headers)
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return json.loads(response.content)


def main():
    print(get(
        'https://bigquery.googleapis.com/bigquery/v2/projects/bigquery-public-data/datasets?all=true'))


if __name__ == '__main__':
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/public.DESKTOP-5H03UEQ/Documents/IntroDB-35dbe741f4c7.json'
    main()
