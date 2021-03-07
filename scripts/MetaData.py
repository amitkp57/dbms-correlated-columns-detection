import json
import pathlib

import RESTUtil
import Constants


def get_datasets():
    """
    Get list of datasets from google bigquery public data
    @return:
    """
    response = RESTUtil.get(Constants.DATASET_LIST_SERVICE_URL)
    datasets = [dataset['id'] for dataset in response['datasets']]

    with open(f'{pathlib.Path().absolute().parent}/data/datasets.txt',
              'w') as file:
        file.write('\n'.join(datasets))

    return datasets


def get_tables():
    """
    Get list of tables
    @return:
    """
    tables = []
    with open(f'{pathlib.Path().absolute().parent}/data/datasets.txt',
              'r') as file:
        for dataset in file:
            if any(dataset.startswith(prefix) for prefix in
                   ['bigquery-public-data:covid19', 'bigquery-public-data:geo_census']):
                dataset_id = dataset.split(':')[1].lstrip().rstrip()
                response = RESTUtil.get(
                    Constants.DATASET_TABLE_LIST_SERVICE_URL % dataset_id)
                tables.extend([table['id'] for table in response['tables']])

    with open(f'{pathlib.Path().absolute().parent}/data/tables.txt',
              'w') as file:
        file.write('\n'.join(tables))

    return tables


def get_table_columns():
    """
    Get list of table columns
    @return:
    """
    tables = dict()

    with open(f'{pathlib.Path().absolute().parent}/data/tables.txt',
              'r') as file:
        for fully_qualified_table_name in file:
            fully_qualified_table_name = fully_qualified_table_name.lstrip().rstrip()
            splits = fully_qualified_table_name.split(':')[1].split('.')
            dataset, table = splits
            response = RESTUtil.get(
                Constants.DATASET_TABLE_DETAILS_SERVICE_URL % (dataset, table))
            columns = response['schema']['fields']
            tables[fully_qualified_table_name] = columns

    with open(f'{pathlib.Path().absolute().parent}/data/columns.json',
              'w') as file:
        json.dump(tables, file)

    return tables


def main():
    # print(get_datasets())
    # print(get_tables())
    print(get_table_columns())


if __name__ == '__main__':
    main()
