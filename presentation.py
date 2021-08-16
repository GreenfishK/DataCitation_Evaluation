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

"""
Plot dataframes
"""

fig1 = plt.figure()
ax1 = fig1.add_subplot(111)
ax2 = ax1.twinx()

df_small_retl_ts_i_simp_trp = df_small_retl_ts_i_simp['cnt_triples_db'].unstack(level=2)
df_small_retl_ts_i_simp_trp_ds = df_small_retl_ts_i_simp['cnt_triples_dataset'].unstack(level=2)
df_small_retl_ts_i_simp_time = df_small_retl_ts_i_simp['time_in_seconds'].unstack(level=2)

df_small_retl_ts_i_simp_trp.xs('small', level=1).plot(kind='bar', ax=ax1)
df_small_retl_ts_i_simp_time.xs('small', level=1).plot(ax=ax2,  color=['darkblue', 'orangered'])
df_small_retl_ts_i_simp_trp_ds.xs('small', level=1).plot(kind='bar', ax=ax1, color=['grey'])


ax1.set_title("Runtime performance for the small dataset when querying live data - Q_PERF vs MEM_SAV")
ax1.set_xlabel("Increment")
ax1.set_ylabel("Total number of triples in database (colored) \n Number of triples in dataset (grey)")
ax2.set_ylabel("Runtime (in seconds)")

ax1.title.set_size(20)
ax1.xaxis.label.set_size(16)
ax1.yaxis.label.set_size(16)
ax2.yaxis.label.set_size(16)

ax = plt.gca().set_xticklabels(np.arange(1, 11))
plt.show()

# save the plot as a file
"""fig.savefig('two_different_y_axis_for_single_python_plot_with_twinx.jpg',
            format='jpeg',
            dpi=100,
            bbox_inches='tight')"""

df_big = pd.read_csv("evaluation_results_v20210815_big_0.csv", delimiter=";", index_col=[0, 1, 2, 3, 4, 5])

