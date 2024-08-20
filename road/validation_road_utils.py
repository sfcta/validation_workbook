import pandas as pd

def compute_errors(est_df, obs_df, times):
    error_df = est_df[times] - obs_df[times]
    error_squared_df = error_df.pow(2)
    error_percent_df = error_df.divide(obs_df[times]).fillna(0)
    error_percent_df = error_percent_df.apply(lambda x: x * 100, axis=1)
    return error_df, error_squared_df, error_percent_df

def combine_dataframes(est_df, obs_df, error_df, error_squared_df, error_percent_df, combined_df_cols, chosen_timeperiod):
    combined_df = pd.concat([est_df, obs_df, error_df, error_squared_df, error_percent_df], axis=1)
    select_time_period_df = combined_df.filter(like=chosen_timeperiod)
    specific_columns_combined_df = combined_df[combined_df_cols]
    select_time_period_loc_df = pd.concat([specific_columns_combined_df, select_time_period_df], axis=1)
    return select_time_period_loc_df
