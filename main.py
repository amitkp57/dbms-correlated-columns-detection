import os

import google.auth
import google.auth.transport.requests
import requests


def get_auth_token():
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = "/home/amit/Downloads/amit-pradhan-compute-f61ddefef705.json"

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


def main():
    key = 'AIzaSyDjMXUNSD1-1_oxT2hk5Ow4uaPg0IeCBck'
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/bigquery-public-data/datasets?all=true&key={key}'
    bearer = get_auth_token()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8',
               'Authorization': 'Bearer ' + bearer}
    # print(url, headers)
    response = requests.get(url, headers=headers)
    print(response.json())


if __name__ == '__main__':
    main()
