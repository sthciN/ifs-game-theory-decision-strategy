from .helpers import BigqieryData
from .pca import principal_indicators
from itertools import product
import numpy as np

def nash_equiblirium(connection_id, bigquery_dataset_id, project_id):
    bq = BigqieryData(project_id=project_id,
                connection_id=connection_id,
                bigquery_dataset_id=bigquery_dataset_id
        )
    
    bqd = bq.get_tabledata(bigquery_table_name="ifs_table")
    principal_indicator_codes = principal_indicators(bqd, 500)
    print('*'*100)
    print('*'*100)
    print('principal_indicator_codes', principal_indicator_codes)
    print('*'*100)
    print('*'*100)
    players = []
    strategies = []
    payoff_matrices = []
    nash_equilibria = []
    for player1, player2 in product(players, repeat=2):
        payoff_matrix = payoff_matrices[(player1, player2)]
        row_max = np.max(payoff_matrix, axis=1)
        col_max = np.max(payoff_matrix, axis=0)
        max_row_idxs = np.where(payoff_matrix == row_max.reshape(-1, 1))
        max_col_idxs = np.where(payoff_matrix == col_max.reshape(1, -1))
        equilibria = set(zip(max_row_idxs[0], max_row_idxs[1])) & set(zip(max_col_idxs[0], max_col_idxs[1]))
        nash_equilibria.extend([(player1, player2, strategies[row], strategies[col]) for row, col in equilibria])

    return True
