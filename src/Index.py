import os
import pickle


def get_correlation(col1, col2):
    """
    Return correlation between col1 and col2
    @param col1:
    @param col2:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/jaccard.obj'
    with open(file_path, 'rb') as file:
        jaccard = pickle.load(file)
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/pandas_correlation.obj'
    with open(file_path, 'rb') as file:
        pearson = pickle.load(file)
    jaccard_columns = set(jaccard.columns)
    pearson_columns = set(pearson.columns)
    if col1 in jaccard_columns and col2 in jaccard_columns:
        return jaccard[col1][col2]
    elif col1 in pearson_columns and col2 in pearson_columns:
        return pearson[col1][col2]
    else:
        print(
            f'Correlation calculation between numerical and string type columns is not supported. Given columns: {col1} and {col2}.')
    return


def get_top_k(col1, k=10):
    """
    Returns top k columns with max pearson/jaccard correlation value
    @param col1:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/jaccard.obj'
    with open(file_path, 'rb') as file:
        jaccard = pickle.load(file)
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/pandas_correlation.obj'
    with open(file_path, 'rb') as file:
        pearson = pickle.load(file)
    jaccard_columns = set(jaccard.columns)
    pearson_columns = set(pearson.columns)
    if col1 in jaccard_columns:
        return jaccard[col1].nlargest(k).index.values
    elif col1 in pearson_columns:
        return pearson[col1].nlargest(k).index.values
    else:
        print(f'Invalid column name {col1}!')
        return


def get_greater_than_threshold(col1, threshold=0.8):
    """
    Returns all columns with pearson/jaccard correlation values greater than the threshold value
    @param threshold:
    @param col1:
    @return:
    """
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/jaccard.obj'
    with open(file_path, 'rb') as file:
        jaccard = pickle.load(file)
    file_path = f'{os.environ["WORKING_DIRECTORY"]}/results/pandas_correlation.obj'
    with open(file_path, 'rb') as file:
        pearson = pickle.load(file)
    jaccard_columns = set(jaccard.columns)
    pearson_columns = set(pearson.columns)
    if col1 in jaccard_columns:
        return jaccard.index[jaccard[col1] > threshold].tolist()
    elif col1 in pearson_columns:
        return pearson.index[pearson[col1] > threshold].tolist()
    else:
        print(f'Invalid column name {col1}!')
        return


def main():
    pass


if __name__ == '__main__':
    main()
