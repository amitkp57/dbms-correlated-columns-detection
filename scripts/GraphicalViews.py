import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.style.use('ggplot')


def plot(plot_name, corr_2d, corr_th=0.8):
    # params
    mat_len = len(corr_2d)
    corr_mat = corr_2d  # input correlation matrix
    use_label = False  # use label on each cell

    column_names = corr_2d.columns
    # exclude_types = ['STRING', 'GEOGRAPHY', 'DATE', 'DATETIME', 'TIMESTAMP']
    # all_cols = qd.get_columns_exclude(exclude_types)

    col_names_i = []
    col_names_j = []
    len_i = len(corr_2d)
    len_j = len(corr_2d)
    if corr_th > -1.0:
        th_i = []
        th_j = []
        for i in range(len_i):
            i_added = False
            for j in range(i + 1, len_j):
                if corr_mat[column_names[i]][column_names[j]] > corr_th:
                    if i not in th_i:
                        th_i.append(i)
                    if j not in th_j:
                        th_j.append(j)

        th_mat = []
        for i in th_i:
            temp = []
            for j in th_j:
                temp.append(corr_mat[column_names[i]][column_names[j]])
            th_mat.append(temp)
        corr_mat = th_mat

        len_i = len(th_i)
        len_j = len(th_j)

        for i in th_i:
            col_names_i.append(column_names[i])
        for j in th_j:
            col_names_j.append(column_names[j])
    else:
        for i in range(len_i):
            col_names_i.append(column_names[i])
        col_names_j = col_names_i

    plt.rcParams["figure.figsize"] = (len_j / 3, len_i / 3)  # image size

    fig, ax = plt.subplots()
    im = ax.imshow(corr_mat)
    im.set_clim(-1, 1)
    ax.grid(False)
    ax.set_xticks(range(len_j))
    ax.set_xticklabels(col_names_i, rotation=45, ha='right')
    ax.yaxis.set(ticks=range(len_i), ticklabels=col_names_j)
    ax.set_ylim(len_i - 0.5, -0.5)
    if use_label:
        for i in range(mat_len):
            for j in range(mat_len):
                ax.text(j, i, '%.2f' % corr_2d[i][j], ha='center', va='center',
                        color='r')
    cbar = ax.figure.colorbar(im, ax=ax, format='% .2f')
    # plt.show()
    plt.savefig(f'{os.environ["WORKING_DIRECTORY"]}/results/{plot_name}.png')
    return


def main():
    df = pd.DataFrame(np.random.randint(0, 100, size=(5, 5)), index=list('ABCDE'), columns=list('ABCDE'))
    plot(df, corr_th=0.8)


if __name__ == '__main__':
    main()
