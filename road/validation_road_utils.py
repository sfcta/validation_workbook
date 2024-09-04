import pandas as pd

def compute_and_combine(est_df, obs_df, times, combined_df_cols, chosen_timeperiod):
    combined_df = (
        pd.concat([est_df[times], obs_df[times]], axis=1, keys=["Estimated Volume", "Observed Volume"])
        .assign(
            errors=lambda df: df["Estimated Volume"] - df["Observed Volume"],
            squared_errors=lambda df: df["errors"].pow(2),
            error_percent=lambda df: df["errors"].divide(df["Observed Volume"]).fillna(0).mul(100)
        )
    )
    # Now filter by the selected time period and only keep relevant columns
    select_time_period_df = combined_df.filter(like=chosen_timeperiod)
    specific_columns_combined_df = combined_df[combined_df_cols]
    select_time_period_loc_df = pd.concat([specific_columns_combined_df, select_time_period_df], axis=1)
    
    return select_time_period_loc_df