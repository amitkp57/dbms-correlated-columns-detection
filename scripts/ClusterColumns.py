import logging
import os
import pathlib

import numpy as np
from datasketch import MinHashLSHForest, MinHash, MinHashLSH

import scripts.QueryDatabase as queryDatabase

NUM_PERM = 128

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


def tokenize(values):
    """
    Tokenize each string in the values array. Return a flattened array of tokens.
    @param values:
    @return:
    """
    tokens = []
    # nlp = spacy.load('en_core_web_sm', disable=['tagger', 'parser', 'ner'])
    for value in values:
        if value is not None:
            # tokens.extend(list(nlp(value)))
            tokens.extend(value.lower().split())
    return tokens


def build_lsh_forest(columns):
    """
    Builds a minHash LSH forest which can be used to query top-k columns with maximum Jaccard similarity
    @param columns:
    @return:
    """
    forest = MinHashLSHForest(num_perm=NUM_PERM)
    for column in columns:
        values = queryDatabase.get_column_values(column['table'], column['column'])
        tokens = tokenize(values)
        minhash = MinHash(num_perm=NUM_PERM)
        for token in tokens:
            minhash.update(token.encode('utf8'))
        forest.add(f'{column["table"]}.{column["column"]}', minhash)
    forest.index()
    return forest


def build_minhash_lsh(columns, threshold=0.5):
    """
    Builds a minhash LSH which can be used to find columns with Jaccard similarity greater than threshold.
    @param columns:
    @param threshold:
    @return:
    """
    minhash_lsh = MinHashLSH(threshold=threshold, num_perm=NUM_PERM)
    for column in columns:
        values = queryDatabase.get_column_values(column['table'], column['column'])
        tokens = tokenize(values)
        minhash = MinHash(num_perm=NUM_PERM)
        for token in tokens:
            minhash.update(token.encode('utf8'))
        minhash_lsh.insert(f'{column["table"]}.{column["column"]}', minhash)
    return minhash_lsh


def get_top_k(forest, column, k=5):
    """
    Get top k columns with best Jaccard similarity
    @param forest:
    @param column:
    @param k:
    @return:
    """
    table = '.'.join(column.split('.')[:-1])
    column = column.split('.')[-1]
    values = queryDatabase.get_column_values(table, column)
    tokens = tokenize(values)
    minhash = MinHash(num_perm=NUM_PERM)
    for token in tokens:
        minhash.update(token.encode('utf8'))
    return forest.query(minhash, k)


def get_all_similar_columns(minhash_lsh, column):
    """
    Get all similar columns (with Jaccard similarity greater than a threshold value)
    @param minhash_lsh:
    @param column:
    @return:
    """
    table = '.'.join(column.split('.')[:-1])
    column = column.split('.')[-1]
    values = queryDatabase.get_column_values(table, column)
    tokens = tokenize(values)
    minhash = MinHash(num_perm=NUM_PERM)
    for token in tokens:
        minhash.update(token.encode('utf8'))
    return minhash_lsh.query(minhash)


def calculate_jaccard_similarity(columns):
    """
    Calculates Jaccrd similarity between all pairs of columns
    @param columns:
    @return:
    """
    minhash_list = []
    for column in columns:
        values = queryDatabase.get_column_values(column['table'], column['column'])
        tokens = tokenize(values)
        minhash = MinHash(num_perm=NUM_PERM)
        for token in tokens:
            minhash.update(token.encode('utf8'))
        minhash_list.append(minhash)
    n = len(columns)
    matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            matrix[i][j] = minhash_list[i].jaccard(minhash_list[j])
    return matrix


def main():
    string_columns = queryDatabase.get_columns('STRING', limit=10)
    forest = build_lsh_forest(string_columns)
    lsh = build_minhash_lsh(string_columns, 0.7)
    print(
        f'top 10 similar columns to bigquery-public-data.covid19_aha.hospital_beds.state_name: '
        f'{get_top_k(forest, "bigquery-public-data.covid19_aha.hospital_beds.state_name")}')
    print()
    print(calculate_jaccard_similarity(string_columns))


if __name__ == '__main__':
    os.environ["WORKING_DIRECTORY"] = f'{pathlib.Path(__file__).parent.parent}'
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/public.DESKTOP-5H03UEQ/Documents/amit-pradhan-compute-23315413b3a3.json'
    main()
