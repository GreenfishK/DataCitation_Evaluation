import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib import ticker

df_small = pd.read_csv("evaluation_results_v20210815_0.csv", delimiter=";", index_col=[0, 1, 2, 3, 4, 5])

df_small_retrieve_live = df_small[np.in1d(df_small.index.get_level_values(4), ['retrieve_live_data'])]
df_small_retrieve_history = df_small[np.in1d(df_small.index.get_level_values(4), ['retrieve_history_data'])]

df_small_retl_ts_insert = df_small_retrieve_live[np.in1d(df_small_retrieve_live.index.get_level_values(0),
                                                                  ['timestamped_insert'])]
df_small_retl_ts_update = df_small_retrieve_live[np.in1d(df_small_retrieve_live.index.get_level_values(0),
                                                                  ['timestamped_update'])]
df_small_reth_ts_insert = df_small_retrieve_history[np.in1d(df_small_retrieve_history.index.get_level_values(0),
                                                                  ['timestamped_insert'])]
df_small_reth_ts_update = df_small_retrieve_history[np.in1d(df_small_retrieve_history.index.get_level_values(0),
                                                                  ['timestamped_update'])]

df_small_retl_ts_i_simp = df_small_retl_ts_insert[np.in1d(df_small_retl_ts_insert.index.get_level_values(3),
                                                                  ['simple_query'])]
df_small_retl_ts_i_cplx = df_small_retl_ts_insert[np.in1d(df_small_retl_ts_insert.index.get_level_values(3),
                                                                  ['complex_query'])]
df_small_retl_ts_u_simp = df_small_retl_ts_update[np.in1d(df_small_retl_ts_update.index.get_level_values(3),
                                                                  ['simple_query'])]
df_small_retl_ts_u_cplx = df_small_retl_ts_update[np.in1d(df_small_retl_ts_update.index.get_level_values(3),
                                                                  ['complex_query'])]
df_small_reth_ts_i_simp = df_small_reth_ts_insert[np.in1d(df_small_reth_ts_insert.index.get_level_values(3),
                                                                  ['simple_query'])]
df_small_reth_ts_i_cplx = df_small_reth_ts_insert[np.in1d(df_small_reth_ts_insert.index.get_level_values(3),
                                                                  ['complex_query'])]
df_small_reth_ts_u_simp = df_small_reth_ts_update[np.in1d(df_small_reth_ts_update.index.get_level_values(3),
                                                                  ['simple_query'])]
df_small_reth_ts_u_cplx = df_small_reth_ts_update[np.in1d(df_small_reth_ts_update.index.get_level_values(3),
                                                                  ['complex_query'])]

df_big = pd.read_csv("evaluation_results_v20210815_big_0.csv", delimiter=";", index_col=[0, 1, 2, 3, 4, 5])

dfs = [df_small_retl_ts_i_simp, df_small_retl_ts_i_cplx, df_small_retl_ts_u_simp, df_small_retl_ts_u_cplx,
       df_small_reth_ts_i_simp, df_small_reth_ts_i_cplx, df_small_reth_ts_u_simp, df_small_reth_ts_u_cplx]

"""
Plot dataframes
"""

index_labels = {'timestamped_insert': 'timestamped insert',
                'timestamped_update': 'timestamped update',
                'retrieve_live_data': 'live data',
                'retrieve_history_data': 'history data',
                'small': 'small',
                'big': 'big',
                'simple_query': 'simple query',
                'complex_query': 'complex query'}

for df in dfs:
    write_operation = index_labels[list(set(df.index.get_level_values(0).tolist()))[0]]
    dataset_size = index_labels[list(set(df.index.get_level_values(1).tolist()))[0]]
    operation = index_labels[list(set(df.index.get_level_values(4).tolist()))[0]]
    query_type = index_labels[list(set(df.index.get_level_values(3).tolist()))[0]]


    fig1 = plt.figure()
    fig1.set_size_inches(32, 18)
    ax1 = fig1.add_subplot(111)
    ax2 = ax1.twinx()

    df_trp = df['cnt_triples_db'].unstack(level=2)
    df_trp_ds = df['cnt_triples_dataset'].unstack(level=2)
    df_time = df['time_in_seconds'].unstack(level=2)

    df_trp.xs('small', level=1).plot(kind='bar', ax=ax1)
    df_time.xs('small', level=1).plot(ax=ax2, color=['darkblue', 'orangered'])
    df_trp_ds.xs('small', level=1).plot(kind='bar', ax=ax1, color=['grey'])

    ax1.set_title("Runtime performance for the {0} dataset when querying {1} with a {2}".format(dataset_size, operation,
                                                                                                query_type))
    ax1.set_xlabel("Increment with {write_operation}".format(write_operation=write_operation))
    ax1.set_ylabel("Total number of triples in database (colored) \n Number of triples in dataset (grey)")
    ax2.set_ylabel("Runtime (in seconds)")

    ax1.title.set_size(20)
    ax1.xaxis.label.set_size(16)
    ax1.yaxis.label.set_size(16)
    ax2.yaxis.label.set_size(16)

    ax = plt.gca().set_xticklabels(np.arange(1, 11))
    plt.show()



