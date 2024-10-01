import pandas as pd

def compute_and_combine_scatter(est_df, obs_df, times, combined_df_cols, chosen_timeperiod):
    # Ensure the chosen_timeperiod is valid
    if chosen_timeperiod not in times:
        raise ValueError(f"Invalid time period: {chosen_timeperiod}. It must be one of {times}.")

    # Compute the error, squared error, and percent error for the chosen time period
    error_df = est_df[chosen_timeperiod] - obs_df[chosen_timeperiod]
    error_squared_df = error_df.pow(2)
    error_percent_df = (error_df / obs_df[chosen_timeperiod]).fillna(0) * 100  # Avoid division by zero

    # Combine the chosen time period's data into a single DataFrame
    combined_df = pd.concat(
        [est_df[combined_df_cols], est_df[chosen_timeperiod], obs_df[chosen_timeperiod], error_df, error_squared_df, error_percent_df],
        axis=1
    )

    # Rename columns for clarity
    combined_df.columns = (
        combined_df_cols + 
        [f"{chosen_timeperiod}_estimated", f"{chosen_timeperiod}_observed", f"{chosen_timeperiod}_error", 
         f"{chosen_timeperiod}_squared_error", f"{chosen_timeperiod}_percent_error"]
    )

    return combined_df

def compute_and_combine_stats(est_df, obs_df, times, combined_df_cols):
    # Create an empty DataFrame to store the combined data for all time periods
    combined_df = est_df[combined_df_cols].copy()  # Start with the base columns from est_df
    
    # Iterate through each time period in the list `times`
    for period in times:
        # Compute the error, squared error, and percent error for the current period
        error_df = est_df[period] - obs_df[period]
        error_squared_df = error_df.pow(2)
        error_percent_df = (error_df / obs_df[period]).fillna(0) * 100  # Avoid division by zero

        # Add each period's data as new columns in the combined DataFrame
        combined_df = pd.concat(
            [combined_df, est_df[period], obs_df[period], error_df, error_squared_df, error_percent_df],
            axis=1
        )

        # Rename columns for clarity
        combined_df.columns = list(combined_df.columns[:-5]) + [
            f"{period}_estimated", f"{period}_observed", f"{period}_error", 
            f"{period}_squared_error", f"{period}_percent_error"
        ]

    return combined_df