# Calculate Pearson correlation among columns

import os
import pathlib
import pickle

import numpy as np
import pandas as pd
from scipy import stats

from src import MetaData, QueryDatabase


def get_table_values(override=False, sample_size=10000):
    """
    Returns an array of column values of given sample size
    @type override: object
    @return:
    """
    column_val_path = f'{os.environ["WORKING_DIRECTORY"]}/results/sampled_columns.obj'
    table_names_path = f'{os.environ["WORKING_DIRECTORY"]}/results/table_names.obj'
    if not override and os.path.isfile(column_val_path) and os.path.isfile(table_names_path):
        with open(column_val_path, 'rb') as file:
            columns = pickle.load(file)
        with open(table_names_path, 'rb') as file:
            table_names = pickle.load(file)
        return columns, table_names

    tables = MetaData.get_tables(f'{os.environ["WORKING_DIRECTORY"]}/data/datasets.txt')
    columns = []
    table_names = []
    count = 0
    for table in tables:
        table_name = table.replace(":", ".")
        table_path = f'{os.environ["WORKING_DIRECTORY"]}/data/tables/{table_name}.npy'
        if not os.path.isfile(table_path):
            print(f'table {table_name} does not have numeric columns.')
            continue
        with open(table_path, 'rb') as file:
            table_data = np.load(file, allow_pickle=True)
            if table_data.shape[0] > sample_size:
                table_data = table_data[np.random.default_rng().choice(table_data.shape[0], sample_size, replace=False)]
            for col in np.transpose(table_data):
                columns.append(col.astype('float64'))
                table_names.append(table_name)
            count += 1
            print(f'Loaded {count} tables.')

    with open(column_val_path, 'wb') as file:
        pickle.dump(columns, file)
    with open(table_names_path, 'wb') as file:
        pickle.dump(table_names, file)

    return columns, table_names


def calculate_pearson_correlation(override=False, sample_size=10000, num_permutations=10):
    """
    Calculate Pearson correlation for each column combination
    @param override:
    @param sample_size:
    @param num_permutations:
    """
    columns, table_names = get_table_values(override=override, sample_size=sample_size)
    corr_matrix = np.zeros((len(columns), len(columns)))
    count = 0
    for i in range(len(columns)):
        for j in range(i + 1, len(columns)):
            len_i = len(columns[i])
            len_j = len(columns[j])
            col_i = columns[i]
            col_j = columns[j]
            table_name_i = table_names[i]
            table_name_j = table_names[j]
            if table_name_i != table_name_j:
                correlation = 0
                for _ in range(num_permutations):
                    col_i = col_i[np.random.default_rng().choice(col_i.shape[0], min(len_i, len_j), replace=False)]
                    col_j = col_j[np.random.default_rng().choice(col_j.shape[0], min(len_i, len_j), replace=False)]
                    correlation = max(correlation, stats.pearsonr(col_i, col_j)[0])
            else:
                correlation = stats.pearsonr(col_i, col_j)[0]
            corr_matrix[i][j] = correlation
            count += 1
            if count % 10000 == 0:
                print(f'Completed {count} correlation calculations.')

    return corr_matrix


def save_corr_matrix(override=False, sample_size=10000, num_permutations=10):
    """
    Saves correlation matrix locally
    @param override:
    @param sample_size:
    @param num_permutations:
    @return:
    """
    corr_matrix = calculate_pearson_correlation(override=override, sample_size=sample_size,
                                                num_permutations=num_permutations)
    np.savez(f'{os.environ["WORKING_DIRECTORY"]}/results/corr_matrix.npz', corr_matrix=corr_matrix)
    return corr_matrix


def get_corr_matrix(override=False, sample_size=10000, num_permutations=10):
    """
    Get correlation matrix from saved file if present else calculate
    @param override:
    @param sample_size:
    @param num_permutations:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/corr_matrix.npz'
    if override or not os.path.isfile(file_path):
        corr_matrix = save_corr_matrix(override=override, sample_size=sample_size, num_permutations=num_permutations)
    else:
        corr_matrix = np.load(file_path)['corr_matrix']
    return corr_matrix


def calculate_pandas_correlation():
    """
    Calculates Pearson correlation values for each column combination using pandas dataframe
    @return: 
    """
    tables = MetaData.get_tables(f'{os.environ["WORKING_DIRECTORY"]}/data/datasets.txt')
    dfs = []
    for table in tables:
        table_name = table.replace(":", ".")
        table_path = f'{os.environ["WORKING_DIRECTORY"]}/data/tables/{table_name}.npy'
        if not os.path.isfile(table_path):
            print(f'table {table_name} does not have numeric columns.')
            continue
        with open(table_path, 'rb') as file:
            columns = QueryDatabase.get_table_columns(table_name, exclude_types=['GEOGRAPHY', 'STRING'])
            column_names = list(map(lambda column: column['table'] + "." + column['column'], columns))
            dfs.append(pd.DataFrame(np.load(file, allow_pickle=True), columns=column_names))
    df = pd.concat(dfs, axis=1)
    return df.corr(method='pearson')


def get_pandas_correlation(override=False):
    """
    Get pandas correlation matrix
    @param override:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/pandas_correlation.obj'
    if not override and os.path.isfile(file_path):
        with open(file_path, 'rb') as file:
            correlations = pickle.load(file)
        return correlations
    correlations = calculate_pandas_correlation()
    with open(file_path, 'wb') as file:
        pickle.dump(correlations, file)
    return correlations


def main():
    pass


if __name__ == '__main__':
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/public.DESKTOP-5H03UEQ/Documents/IntroDB-35dbe741f4c7.json'
    main()
