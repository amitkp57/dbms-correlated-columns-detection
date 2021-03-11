# Util methods to query Google BigQuery

import json
import os
import pathlib
from collections import defaultdict

import numpy as np
import pandas as pd
from google.cloud import bigquery

# We will find correlations only among the below types of columns
from scripts import MetaData

COLUMN_TYPES = ['STRING', 'INTEGER', 'DATE', 'FLOAT', 'DATETIME', 'TIMESTAMP', 'NUMERIC', 'BOOLEAN', 'GEOGRAPHY']


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
            output.append({'table': table.replace(':', '.'), 'column': column['name'], 'type': column['type']})
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
                output.append({'table': table.replace(':', '.'), 'column': column['name'], 'type': column['type']})
    return output[:limit]


def get_table_columns(table, exclude_types=[]):
    """
    Returns the list of columns for a given table. Excludes the columns whose type is in the exclude_types.
    @param table:
    @param exclude_types:
    @return:
    """
    with open(f'{os.environ["WORKING_DIRECTORY"]}/data/columns.json') as file:
        table_columns = json.load(file)

    # json file contains table name in <project>:<dataset>.<format> but query table name in <project>.<dataset>.<format>
    # format
    table_columns = table_columns[table.replace('.', ':', 1)]

    output = []
    for column in table_columns:
        if column['type'] not in exclude_types:
            output.append({'table': table, 'column': column['name'], 'type': column['type']})
    return output


def get_columns_exclude(column_types, limit=10000):
    """
    Get the list of columns while excluding the given type. Size of returned columns will be limited by given limit value.
    @param column_type:
    @param limit:
    """
    for column_type in column_types:
        if column_type not in COLUMN_TYPES:
            raise ValueError(f'{column_type} is not a valid type for correlation finding!')

    with open(f'{os.environ["WORKING_DIRECTORY"]}/data/columns.json') as file:
        table_columns = json.load(file)

    output = []
    for table, columns in table_columns.items():
        for column in columns:
            if column['type'] != column_type and column['type'] in COLUMN_TYPES:
                output.append({'table': table.replace(':', '.'), 'column': column['name'], 'type': column['type']})
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


def transform_columns(dt, columns):
    """
    Converts date, datetime, timestamp and boolean type columns to int64
    @param dt:
    @param columns:
    @return:
    """
    for column in columns:
        column_name = column['column']
        if column['type'] in ['DATE', 'DATETIME', 'TIMESTAMP']:
            # default to epoch start time
            dt[column_name] = pd.to_datetime(dt[column_name]).fillna(pd.to_datetime('1970-01-01')).astype('int64')
        elif column['type'] in ['BOOLEAN']:
            dt[column_name] = dt[column_name].fillna(False).astype('int64')
    return dt


def get_columns_values(table_name, columns, sampling=0):
    """
    Returns the list of column values for multiple columns from the table
    @param table_name:
    @param columns:
    @return:
    """
    column_names = list(map(lambda column: column['column'], columns))
    client = bigquery.Client(project='introdb-303217')
    if sampling > 0:
        # query row count
        row_count = client.query('''
            SELECT
              COUNT(*) as total
            FROM `%s`''' % table_name).to_dataframe().total[0]
        if row_count > sampling:
            query = f'''select {",".join(column_names)} from `{table_name}` where RAND() < {sampling}/{row_count}'''
        else:
            query = f'''select {",".join(column_names)} from `{table_name}`'''
    else:
        query = f'''select {",".join(column_names)} from `{table_name}`'''
    results = client.query(query).result().to_dataframe()
    results = transform_columns(results, columns)
    return results.to_numpy(na_value=0)


def get_column_values(table_name, column):
    """
    Returns the list of column values from the table
    @param table_name:
    @param column:
    @return:
    """
    client = bigquery.Client(project='introdb-303217')
    column_name = column['column']
    query = f'''select {column_name} from {table_name}'''
    results = client.query(query).result().to_dataframe()
    results = transform_columns(results, [column])
    return results.to_numpy().ravel()


def get_distnct_column_values(table_name, column):
    """
    Returns the list of distinct column values from the table
    @param table_name:
    @param column:
    @return:
    """
    client = bigquery.Client(project='introdb-303217')
    column_name = column['column']
    query = f'''select DISTINCT {column_name} from {table_name}'''
    results = client.query(query).result().to_dataframe()
    results = transform_columns(results, [column])
    return results.to_numpy().ravel()


def save_table_column_values(table, sample_size=100000, override=False):
    """
    Queries Google bigquery to randomly sample values of the given sample size and saves locally
    @param override:
    @param table:
    @param sample_size:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/data/tables/{table}.npy'
    if override or not os.path.isfile(file_path):
        columns = get_table_columns(table, exclude_types=['GEOGRAPHY'])
        result = get_columns_values(table, columns, sample_size)
        with open(file_path, 'wb') as file:
            np.save(file, result)
    return


def save_all_table_data(override=False, sample_size=100000):
    """
    Saves all table data locally
    @param override:
    @param sample_size:
    @return:
    """
    tables = MetaData.get_tables(f'{os.environ["WORKING_DIRECTORY"]}/data/datasets.txt')
    for table in tables:
        save_table_column_values(table.replace(':', '.'), sample_size=sample_size, override=override)
    return


def main():
    # print(get_columns(100))
    # print(get_all_column_types())
    # print(get_columns('STRING'))
    # print(len(get_column_values('bigquery-public-data.covid19_aha.hospital_beds', 'state_name')))
    # print(get_table_columns('bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide'))
    save_table_column_values('bigquery-public-data.covid19_nyt.excess_deaths')


if __name__ == '__main__':
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/public.DESKTOP-5H03UEQ/Documents/IntroDB-35dbe741f4c7.json'
    main()
