import pandas as pd

def compute_and_combine_scatter(est_df, obs_df, times, combined_df_cols, chosen_timeperiod):
    # Ensure the chosen_timeperiod is valid
    if chosen_timeperiod not in times:
        raise ValueError(f"Invalid time period: {chosen_timeperiod}. It must be one of {times}.")

    # Compute the error, squared error, and percent error for the chosen time period
    error_df = est_df[chosen_timeperiod] - obs_df[chosen_timeperiod]
    error_squared_df = error_df.pow(2)
    error_percent_df = (error_df / obs_df[chosen_timeperiod]).fillna(0) * 100  # Avoid division by zero

    # Combine the chosen time period's data into a single DataFrame using `assign`
    combined_df = est_df[combined_df_cols].assign(
        **{
            f"{chosen_timeperiod}_estimated": est_df[chosen_timeperiod],
            f"{chosen_timeperiod}_observed": obs_df[chosen_timeperiod],
            f"{chosen_timeperiod}_error": error_df,
            f"{chosen_timeperiod}_squared_error": error_squared_df,
            f"{chosen_timeperiod}_percent_error": error_percent_df
        }
    )

    return combined_df


def compute_and_combine_stats(est_df, obs_df, times, combined_df_cols):
    # Create a base DataFrame using the specified columns
    combined_df = est_df[combined_df_cols].copy()

    if 'Observed Volume Category' in obs_df.columns:
        combined_df['Observed Volume Category'] = obs_df['Observed Volume Category'].values
    # Iterate through each time period in the list `times`
    for period in times:
        # Compute the error, squared error, and percent error for the current period
        error_df = est_df[period] - obs_df[period]
        error_squared_df = error_df.pow(2)
        error_percent_df = (error_df / obs_df[period]).fillna(0) * 100  # Avoid division by zero

        # Use `assign` to add new columns for each time period
        combined_df = combined_df.assign(
            **{
                f"{period}_estimated": est_df[period],
                f"{period}_observed": obs_df[period],
                f"{period}_error": error_df,
                f"{period}_squared_error": error_squared_df,
                f"{period}_percent_error": error_percent_df
            }
        )

    return combined_df
