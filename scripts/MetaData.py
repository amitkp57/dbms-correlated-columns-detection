import pathlib

import RESTUtil
import constants


def get_datasets():
    response = RESTUtil.get(constants.DATASET_SERVICE_URL)
    datasets = [dataset['id'] for dataset in response['datasets']]
    with open(f'{pathlib.Path().absolute().parent}/data/datasets.txt',
              'w') as file:
        file.write('\n'.join(datasets))
    return datasets


def get_tables():
    tables = []
    with open(f'{pathlib.Path().absolute().parent}/data/datasets.txt',
              'r') as file:
        for dataset in file:
            if dataset.startswith('bigquery-public-data:covid19'):
                dataset_id = dataset.split(':')[1].lstrip().rstrip()
                response = RESTUtil.get(
                    constants.DATASET_TABLE_SERVICE_URL % dataset_id)
                tables.extend([table['id'] for table in response['tables']])
    with open(f'{pathlib.Path().absolute().parent}/data/tables.txt',
              'w') as file:
        file.write('\n'.join(tables))
    return tables


def main():
    # print(get_datasets())
    print(get_tables())


if __name__ == '__main__':
    main()
