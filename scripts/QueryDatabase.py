import json
import os
import pathlib


def get_columns(limit):
    """
    Get the list of columns. Size of returned columns will be limited by given limit value.
    @param limit:
    """
    table_columns = json.read()


def main():
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'


if __name__ == '__main__':
    main()
