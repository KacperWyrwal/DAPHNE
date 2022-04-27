from data_processing_per_trial import get_visits, get_raw_data, reindex_and_interpolate, remove_outliers, smoothen, \
    merge_visits, \
    save_pcmci_results, transform_pandas_to_tigramite, get_processed_data, save_preprocessed_data, \
    get_missing_data_ratio

import pandas as pd
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr, CMIknn


def preprocess(ID, path_from, path_to, airspeckp_interpolation_limit=5, interpolation_method='linear',
               respeck_interpolation_limit=0):
    visits = get_visits(ID=ID, path=path_from)
    dfs = []

    # Preprocess each visit independently
    for visit in visits:
        airspeckp, respeck = get_raw_data(ID=ID, visit=visit, path=path_from)

        if airspeckp_interpolation_limit > 0:
            airspeckp = reindex_and_interpolate(df=airspeckp, index=respeck.index, method=interpolation_method,
                                                limit=airspeckp_interpolation_limit)
        if respeck_interpolation_limit > 0:
            respeck.interpolate(method=interpolation_method, limit=respeck_interpolation_limit, inplace=True)

        visit_df = pd.merge(left=airspeckp, right=respeck, left_index=True, right_index=True)

        visit_df = remove_outliers(df=visit_df, rise_length=1, peak_width=1)
        visit_df = smoothen(df=visit_df, window=30)

        dfs.append(visit_df)

    # merge all visits together and save
    df = merge_visits(dfs=dfs)
    save_preprocessed_data(data=df, ID=ID, path=path_to)


def run_pipeline(ID, path_from, path_to, cond_ind_test='ParCorr', tau_max=60, fdr_method='fdr_bh', per_trial=True,
                 data_length_ratio=1.0):

    df = get_processed_data(ID=ID, path=path_from)

    # divide data into trials and analyse them separately if per_trial is True
    if per_trial:
        dfs = df.groupby('trial')
    else:
        dfs = [('all', df)]

    for trial, df in dfs:
        df = df.drop('trial', axis=1)

        # reduce length for analysis speed-up
        if 0.0 < data_length_ratio < 1.0:
            df = df.iloc[int(len(df) * data_length_ratio)]

        missing_data_ratio = get_missing_data_ratio(df=df, tau_max=tau_max)

        print(f"{trial=}")
        print(f"{missing_data_ratio=}")
        if missing_data_ratio == 1.0:
            print("There are no unmasked samples for PCMCI+ to run on.")
            return

        df = transform_pandas_to_tigramite(df=df)

        # Setup PCMCI object
        cond_ind_test_object = get_cond_ind_test(cond_ind_test=cond_ind_test)
        pc_alpha = get_pc_alpha(cond_ind_test=cond_ind_test)
        pcmci = PCMCI(dataframe=df, cond_ind_test=cond_ind_test_object)
        var_names = df.var_names
        selected_links = get_selected_links(var_names=var_names, tau_max=tau_max)

        # Run analysis
        results = pcmci.run_pcmciplus(tau_max=tau_max, fdr_method=fdr_method, pc_alpha=pc_alpha,
                                      selected_links=selected_links)
        save_pcmci_results(results=results, var_names=var_names, tau_max=tau_max, cond_ind_test=cond_ind_test,
                           ID=ID, trial=trial, missing_data_ratio=missing_data_ratio, path=path_to)


def get_pc_alpha(cond_ind_test):
    if cond_ind_test == 'CMIknn':
        return 0.01
    if cond_ind_test == 'ParCorr':
        return 0.01


def get_cond_ind_test(cond_ind_test):
    if cond_ind_test == 'ParCorr':
        return ParCorr(significance='analytic')
    if cond_ind_test == 'CMIknn':
        return CMIknn(knn=0.1, shuffle_neighbors=10, significance='shuffle_test', transform='ranks',
                      workers=-1)


def get_selected_links(var_names, tau_max):
    # all these dictionaries ensure that even if var_names are in different order than keys in var_to_parents,
    # the selected links are still correctly constructed
    var_to_parents = {
            'breathing_rate': ['breathing_rate', 'activity_level', 'temperature', 'humidity', 'pm2_5'],
            'activity_level': ['breathing_rate', 'activity_level'],
            'temperature': ['temperature', 'humidity', 'pm2_5'],
            'humidity': ['temperature', 'humidity', 'pm2_5'],
            'pm2_5': ['temperature', 'humidity', 'pm2_5'],
        }
    var_to_index = dict(zip(var_names, range(len(var_names))))
    return {index: [(var_to_index[parent], -tau) for parent in var_to_parents[var]
                    for tau in range(0, tau_max+1)]
            for var, index in var_to_index.items()}
