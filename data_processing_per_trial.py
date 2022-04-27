import os
import itertools
import pandas as pd
import numpy as np
import pickle
import re

from tigramite import data_processing as pp


##### Data retrieval #####

def get_processed_data(ID, path):
    index_col = 'timestamp'
    usecols = ['timestamp', 'breathing_rate', 'activity_level', 'temperature', 'humidity', 'pm2_5', 'trial']
    filename = f"{ID}.csv"
    path_with_filename = os.path.join(path, filename)
    return pd.read_csv(path_with_filename, parse_dates=[index_col], index_col=index_col, usecols=usecols)


def get_visits(ID: str, path: str) -> [int]:
    """
    Find all visits for which data is available for a patient
    :param ID: ID of the patient e.g. INH001 or DAP001
    :param path: path to directory where data is located
    :return: sorted list of numbers of visits
    """
    all_available_visits = set()
    for file in os.listdir(path):
        if file.startswith(ID) and file.endswith('.csv'):
            match = re.match(f'{ID}\((?P<visit>\d+)\)', file)
            if match is not None:
                all_available_visits.add(match['visit'])
    return sorted(list(all_available_visits))


def get_raw_data(ID, visit, path):
    airspeckp_data = get_airspeckp(ID=ID, visit=visit, path=path)
    respeck_data = get_respeck(ID=ID, visit=visit, path=path)
    return airspeckp_data, respeck_data


def get_airspeckp(ID, visit, path):
    index_col = 'timestamp'
    usecols = ['timestamp', 'temperature', 'humidity', 'pm2_5']
    filename = f"{ID}({visit})_airspeck_personal_manual_raw.csv"
    path_with_filename = os.path.join(path, filename)
    return pd.read_csv(path_with_filename, parse_dates=[index_col], index_col=index_col, usecols=usecols)


def get_respeck(ID, visit, path):
    index_col = 'timestamp'
    usecols = ['timestamp', 'breathing_rate', 'activity_level']
    filename = f"{ID}({visit})_respeck_manual.csv"
    path_with_filename = os.path.join(path, filename)
    return pd.read_csv(path_with_filename, parse_dates=[index_col], index_col=index_col, usecols=usecols)


##### data processing #####

def reindex_and_interpolate(df, index, method, limit):
    return df.reindex(df.index.union(index)).interpolate(method=method, limit=limit).reindex(index)


def remove_outliers(df, rise_length, peak_width):
    return df


def smoothen(df, window, min_periods=1):
    return df.rolling(window=window, min_periods=min_periods).mean()


def merge_visits(dfs):
    columns = dfs[0].columns
    # insert an empty row between each df in dfs for pcmci to not treat it as continuous
    nan_row_values = np.full((1, len(columns)), np.nan)
    nan_row = pd.DataFrame(nan_row_values, columns=columns)

    # drop the last nan row since it doesn't separate anything
    return pd.concat(itertools.chain(*zip(dfs, itertools.repeat(nan_row))))[:-1]


def transform_pandas_to_tigramite(df):
    missing_flag = get_missing_flag(df=df)
    return pp.DataFrame(data=df.fillna(missing_flag).values, var_names=df.columns, missing_flag=missing_flag)


# smallest sequence of 9s larger than all values in df
def get_missing_flag(df):
    return 10 ** np.ceil(np.log10(max(df.max()) + 1)) - 1


def get_rejected_mask(df, tau_max):
    na_mask = ~df.isna()
    consecutive_non_na_count = na_mask.cumsum() - na_mask.cumsum().where(~na_mask).ffill().fillna(0)
    mask = (consecutive_non_na_count > tau_max).replace(False, pd.NA).bfill(limit=tau_max).fillna(False)
    return ~mask


def get_missing_data_ratio(df, tau_max):
    mask = get_rejected_mask(df=df, tau_max=tau_max)
    
    cols = mask.columns
    mask_or = mask[cols[0]]
    for col in cols[1:]:
        mask_or |= mask[col]

    return 1 - (~mask_or).sum() / len(df)


##### Saving data #####

def save_preprocessed_data(ID, data, path):
    path_with_filename = os.path.join(path, ID)
    with open(path_with_filename, 'wb') as handle:
        pickle.dump(data, handle)


def save_pcmci_results(results, var_names, tau_max, cond_ind_test, ID, trial, missing_data_ratio, path):
    data_to_save = {
        'results': results,
        'var_names': var_names,
        'tau_max': tau_max,
        'cond_ind_test': cond_ind_test,
        'ID': ID,
        'trial': trial,
        'missing_data_ratio': missing_data_ratio,
    }

    # Creates directories in path that don't yet exist
    os.makedirs(path, exist_ok=True)

    filename = f"{ID}"
    if isinstance(trial, (int, float)):
        filename = f"{filename}({int(trial)})"
    if cond_ind_test is not None:
        filename = f"{filename}_{cond_ind_test}"

    path_with_filename = os.path.join(path, filename)
    with open(path_with_filename, 'wb') as handle:
        pickle.dump(data_to_save, handle)
