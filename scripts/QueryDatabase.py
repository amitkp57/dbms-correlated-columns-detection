import json
import os
import pathlib
# Util methods to query Google BigQuery
from collections import defaultdict

from google.cloud import bigquery

# We will find correlations only among the below types of columns
COLUMN_TYPES = ['STRING', 'INTEGER', 'DATE', 'FLOAT', 'DATETIME', 'TIMESTAMP', 'NUMERIC', 'BOOLEAN']


def get_columns(limit=10000):
    """
    Get the list of columns. Size of returned columns will be limited by given limit value.
    @param limit:
    """
    with open(f'{os.environ["WORKING_DIRECTORY"]}/data/columns.json') as file:
        table_columns = json.load(file)

    output = []
    for table, columns in table_columns.items():
        for column in columns:
            output.append({'table': table.replace(':', '.'), 'column': column['name']})
    return output[:limit]


def get_columns(column_type, limit=10000):
    """
    Get the list of columns of the given type. Size of returned columns will be limited by given limit value.
    @param column_type:
    @param limit:
    """
    if column_type not in COLUMN_TYPES:
        raise ValueError(f'{column_type} is not a valid type for correlation finding!')

    with open(f'{os.environ["WORKING_DIRECTORY"]}/data/columns.json') as file:
        table_columns = json.load(file)

    output = []
    for table, columns in table_columns.items():
        for column in columns:
            if column['type'] == column_type:
                output.append({'table': table.replace(':', '.'), 'column': column['name']})
    return output[:limit]


def get_all_column_types():
    """
    Return the distinct column types in the set of the tables
    """
    with open(f'{os.environ["WORKING_DIRECTORY"]}/data/columns.json') as file:
        table_columns = json.load(file)

    output = defaultdict(int)
    for table, columns in table_columns.items():
        for column in columns:
            output[column['type']] += 1
    return output


def get_column_values(table_name, column):
    """
    Returns the list of column values from the table
    @param table_name:
    @param column:
    @return:
    """
    client = bigquery.Client()
    query = f'''select {column} from {table_name}'''
    results = client.query(query).result().to_dataframe().to_numpy().ravel()
    return results


def main():
    # print(get_columns(100))
    # print(get_all_column_types())
    # print(get_columns('STRING'))
    print(len(get_column_values('bigquery-public-data.covid19_aha.hospital_beds', 'state_name')))


if __name__ == '__main__':
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/public.DESKTOP-5H03UEQ/Documents/amit-pradhan-compute-23315413b3a3.json'
    main()
