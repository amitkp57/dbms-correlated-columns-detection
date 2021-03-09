import json
import os
import pathlib

# Util methods to query Google BigQuery
from collections import defaultdict

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
            output.append({'table': table, 'column': column['name']})
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
                output.append({'table': table, 'column': column['name']})
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


def main():
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    # print(get_columns(100))
    # print(get_all_column_types())
    print(get_columns('STRING'))


if __name__ == '__main__':
    main()
