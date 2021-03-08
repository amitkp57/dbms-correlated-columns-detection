import json
import os
import pathlib

import scripts.RESTUtil as RESTUtil
import scripts.Constants as Constants

DATA_PATH = f'{pathlib.Path(__file__).parent.parent}/data'


# Run this file to regenerate meta-data files in the /data folder
def get_datasets():
    """
    Get list of datasets from google bigquery public data
    @return:
    """
    response = RESTUtil.get(Constants.DATASET_LIST_SERVICE_URL)
    datasets = [dataset['id'] for dataset in response['datasets']]
    return datasets


def get_tables(datasets_path):
    """
    Get list of tables
    @return:
    """
    tables = []
    with open(datasets_path, 'r') as file:
        for dataset in file:
            if any(dataset.startswith(prefix) for prefix in
                   ['bigquery-public-data:covid19', 'bigquery-public-data:geo_census']):
                dataset_id = dataset.split(':')[1].lstrip().rstrip()
                response = RESTUtil.get(
                    Constants.DATASET_TABLE_LIST_SERVICE_URL % dataset_id)
                tables.extend([table['id'] for table in response['tables']])
    return tables


def get_table_columns(tables_path):
    """
    Get list of table columns
    @return:
    """
    tables = dict()

    with open(tables_path, 'r') as file:
        for fully_qualified_table_name in file:
            fully_qualified_table_name = fully_qualified_table_name.lstrip().rstrip()
            splits = fully_qualified_table_name.split(':')[1].split('.')
            dataset, table = splits
            response = RESTUtil.get(
                Constants.DATASET_TABLE_DETAILS_SERVICE_URL % (dataset, table))
            columns = response['schema']['fields']
            tables[fully_qualified_table_name] = columns
    return tables


def save_locally(dir):
    """
    Saves datasets, tables and table columns to the given directory
    @param dir:
    """
    datasets = get_datasets()
    with open(f'{dir}/datasets.txt', 'w') as file:
        file.write('\n'.join(datasets))

    tables = get_tables(f'{dir}/datasets.txt')
    with open(f'{dir}/tables.txt', 'w') as file:
        file.write('\n'.join(tables))

    table_columns = get_table_columns(f'{dir}/tables.txt')
    with open(f'{dir}/columns.json', 'w') as file:
        json.dump(table_columns, file)
    return


def main():
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    save_locally(f'{os.environ["WORKING_DIRECTORY"]}/data')


if __name__ == '__main__':
    main()
