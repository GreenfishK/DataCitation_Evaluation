import matplotlib
import pandas as pd
import numpy as np

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


df_small_retl_ts_i_simp_qp = df_small_retl_ts_i_simp[np.in1d(df_small_retl_ts_i_simp.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_retl_ts_i_simp_ms = df_small_retl_ts_i_simp[np.in1d(df_small_retl_ts_i_simp.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_retl_ts_i_cplx_qp = df_small_retl_ts_i_cplx[np.in1d(df_small_retl_ts_i_cplx.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_retl_ts_i_cplx_ms = df_small_retl_ts_i_cplx[np.in1d(df_small_retl_ts_i_cplx.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_retl_ts_u_simp_qp = df_small_retl_ts_u_simp[np.in1d(df_small_retl_ts_u_simp.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_retl_ts_u_simp_ms = df_small_retl_ts_u_simp[np.in1d(df_small_retl_ts_u_simp.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_retl_ts_u_cplx_qp = df_small_retl_ts_u_cplx[np.in1d(df_small_retl_ts_u_cplx.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_retl_ts_u_cplx_ms = df_small_retl_ts_u_cplx[np.in1d(df_small_retl_ts_u_cplx.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_reth_ts_i_simp_qp = df_small_reth_ts_i_simp[np.in1d(df_small_reth_ts_i_simp.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_reth_ts_i_simp_ms = df_small_reth_ts_i_simp[np.in1d(df_small_reth_ts_i_simp.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_reth_ts_i_cplx_qp = df_small_reth_ts_i_cplx[np.in1d(df_small_reth_ts_i_cplx.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_reth_ts_i_cplx_ms = df_small_reth_ts_i_cplx[np.in1d(df_small_reth_ts_i_cplx.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_reth_ts_u_simp_qp = df_small_reth_ts_u_simp[np.in1d(df_small_reth_ts_u_simp.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_reth_ts_u_simp_ms = df_small_reth_ts_u_simp[np.in1d(df_small_reth_ts_u_simp.index.get_level_values(2),
                                                             ['mem_sav'])]
df_small_reth_ts_u_cplx_qp = df_small_reth_ts_u_simp[np.in1d(df_small_reth_ts_u_simp.index.get_level_values(2),
                                                             ['q_perf'])]
df_small_reth_ts_u_cplx_ms = df_small_reth_ts_u_simp[np.in1d(df_small_reth_ts_u_simp.index.get_level_values(2),
                                                             ['mem_sav'])]

print(df_small_reth_ts_i_cplx_ms)

df_big = pd.read_csv("evaluation_results_v20210815_big_0.csv", delimiter=";", index_col=[0, 1, 2, 3, 4, 5])

