import os
import pandas as pd
import numpy as np
import json
from road.validation_road_utils import compute_and_combine_stats

def prepare_time_period_dfs(est_df, obs_df, times, combined_df_cols):
    # Call the compute_and_combine_stats function to get the combined DataFrame
    combined_df = compute_and_combine_stats(est_df, obs_df, times, combined_df_cols)

    # Remove duplicates based on `A` and `B`
    combined_df = combined_df.drop_duplicates(subset=['A', 'B'], keep='first')

    # Columns to include in the time-period-specific DataFrames
    calculation_cols = [
        'Estimated Volume',
        'Observed Volume',
        'Errors',
        'Squared Errors',
        'Percent Errors'
    ]

    time_period_dfs = {}

    for period in times:
        # Select columns specific to the current period
        matching_column_indices = [
            i for i, col_name in enumerate(combined_df.columns) if period in col_name
        ]
        select_time_period_df = combined_df.iloc[:, matching_column_indices]

        # Retain the base columns
        specific_columns_combined_df = combined_df[combined_df_cols + ['Observed Volume Category']]

        # Combine base columns with period-specific columns
        select_time_period_loc_df = pd.concat(
            [specific_columns_combined_df, select_time_period_df], axis=1
        )

        # Rename columns for clarity
        new_column_names = combined_df_cols + ['Observed Volume Category'] + \
            calculation_cols[:len(select_time_period_loc_df.columns) - len(combined_df_cols) - 1]
        select_time_period_loc_df.columns = new_column_names

        time_period_dfs[period] = select_time_period_loc_df

    return time_period_dfs



def classify_observation_volume(volume):
    if volume < 10000:
        return '<10k'
    elif 10000 <= volume < 20000:
        return '10-20k'
    elif 20000 <= volume < 50000:
        return '20-50k'
    else:
        return '>=50k'


# Define function to reset index and rename columns
def reset_index_and_rename(df, group_var):
    df.index.name = group_var
    df = df.reset_index()
    return df


# Define function to reorder DataFrame based on group_var
def reorder_dataframe(df, group_var):
    # Define the default order as None
    order = None
    
    # Determine the correct order based on group_var
    if group_var == 'AT Group':
        order = ['Core/CBD', 'UrbBiz', 'Urb', 'Sub', 'All Locations']
    elif group_var == 'FT Group':
        order = ['Fwy/Ramp', 'Art', 'Col', 'Loc', 'All Locations']
    elif group_var == 'Observed Volume':
        order = ['<10k', '10-20k', '20-50k', '>=50k', 'All Locations']

    # Reorder the DataFrame if a valid order is found
    if order is not None:
        df = df.set_index(group_var)
        df = df.reindex(order)
        df = df.reset_index()

    return df

def calculate_metrics(df, group_var_column, period):
    # Get unique groups for reindexing
    unique_groups = df[group_var_column].unique()

    # Group and calculate aggregated values
    sum_estimated = df.groupby(group_var_column)['Estimated Volume'].sum().reindex(unique_groups, fill_value=0)
    sum_observed = df.groupby(group_var_column)['Observed Volume'].sum().reindex(unique_groups, fill_value=0)

    group = df.groupby(group_var_column).agg(
        sum_squared_errors=('Squared Errors', 'sum'),
        sum_observed=('Observed Volume', 'sum'),
        count=('Loc Type', 'count')
    ).reindex(unique_groups, fill_value=0)

    # Calculate RMSE and percent RMSE
    group['RMSE'] = np.where(
        group['sum_observed'] > 0,
        np.sqrt(group['sum_squared_errors'] / np.maximum(group['count'], 1)) /
        (group['sum_observed'] / np.maximum(group['count'], 1)),
        np.nan
    )
    group['percent_rmse'] = group['RMSE'] * 100

    # Calculate relative error and estimated-to-observed ratio
    relative_error = pd.Series(
        (sum_estimated - sum_observed) / np.where(sum_observed > 0, sum_observed, np.nan),
        index=unique_groups
    )
    est_obs_ratio = pd.Series(
        np.where(sum_observed > 0, (sum_estimated / sum_observed) * 100, np.nan),
        index=unique_groups
    )

    # Calculate total metrics across all groups
    total_sum_squared_errors = df['Squared Errors'].sum()
    total_sum_observed = df['Observed Volume'].sum()
    total_count = df.shape[0]

    total_rmse = np.sqrt(total_sum_squared_errors / total_count) / \
                 (total_sum_observed / total_count if total_sum_observed > 0 else np.nan)
    total_percent_rmse = total_rmse * 100 if total_rmse is not np.nan else np.nan

    total_sum_estimated = df['Estimated Volume'].sum()
    total_relative_error = (total_sum_estimated - total_sum_observed) / total_sum_observed \
        if total_sum_observed > 0 else np.nan
    total_est_obs_ratio = (total_sum_estimated / total_sum_observed) * 100 if total_sum_observed > 0 else np.nan

    return group['percent_rmse'], relative_error, est_obs_ratio, total_percent_rmse, total_relative_error, total_est_obs_ratio



def generate_and_save_tables(outdir, time_period_dfs, group_vars):
    import os

    # Ensure the output directory exists
    os.makedirs(outdir, exist_ok=True)

    # Loop through each group variable
    
    for group_var in group_vars:
        percent_rmse_df = pd.DataFrame()
        relative_error_df = pd.DataFrame()
        est_obs_ratio_df = pd.DataFrame()

        for period, df in time_period_dfs.items():
            # Calculate metrics
            percent_rmse, relative_error, est_obs_ratio, total_percent_rmse, total_relative_error, total_est_obs_ratio = calculate_metrics(
                df, group_var, period
            )
            percent_rmse_df[period] = percent_rmse
            relative_error_df[period] = relative_error
            est_obs_ratio_df[period] = est_obs_ratio
        

            # # Add totals for all locations
            percent_rmse_df.loc['All Locations', period] = total_percent_rmse
            relative_error_df.loc['All Locations', period] = total_relative_error
            est_obs_ratio_df.loc['All Locations', period] = total_est_obs_ratio

        # # Reset index and rename columns
        percent_rmse_df = reset_index_and_rename(percent_rmse_df, group_var)
        relative_error_df = reset_index_and_rename(relative_error_df, group_var)
        est_obs_ratio_df = reset_index_and_rename(est_obs_ratio_df, group_var)

        # # Reorder DataFrame based on group_var
        percent_rmse_df = reorder_dataframe(percent_rmse_df, group_var)
        relative_error_df = reorder_dataframe(relative_error_df, group_var)
        est_obs_ratio_df = reorder_dataframe(est_obs_ratio_df, group_var)


        counts = df[group_var].value_counts()


        # Create a count DataFrame
        count_df = pd.DataFrame(counts).reset_index()
        count_df.columns = [group_var, 'Count']

        # Add total count row
        total_count = count_df['Count'].sum()
        total_row = pd.DataFrame({group_var: ['All Locations'], 'Count': [total_count]})
        count_df = pd.concat([count_df, total_row], ignore_index=True)

        # Reorder and reset the DataFrame for output
        count_df = reorder_dataframe(count_df, group_var)

        # Round the metrics DataFrames for better readability
        percent_rmse_df = percent_rmse_df.applymap(lambda x: f"{x:.1f}" if isinstance(x, (int, float)) else x)
        relative_error_df = relative_error_df.applymap(lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x)
        est_obs_ratio_df = est_obs_ratio_df.applymap(lambda x: f"{x:.3f}" if isinstance(x, (int, float)) else x)

        if group_var == 'Observed Volume Category':
            file_prefix = "observedvolume"
        else:
            file_prefix = f"{group_var.replace(' ', '').lower()}"


        # Save the DataFrames to CSV
        count_df.to_csv(f'{outdir}/{file_prefix}_count.csv', index=False)
        percent_rmse_df.to_csv(f'{outdir}/percent_rmse_{file_prefix}.csv', index=False)
        relative_error_df.to_csv(f'{outdir}/relative_error_{file_prefix}.csv', index=False)
        est_obs_ratio_df.to_csv(f'{outdir}/est_obs_ratio_{file_prefix}.csv', index=False)


        # Melt dataframes for easier plotting or analysis
        melted_percent_rmse_df = percent_rmse_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Percent RMSE'
        )
        melted_relative_error_df = relative_error_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Relative Error'
        )
        melted_est_obs_ratio_df = est_obs_ratio_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Est/Obs Ratio'
        )

        # Save melted dataframes to CSV
        melted_percent_rmse_df.to_csv(
            f'{outdir}/{file_prefix}_percent_rmse_melted.csv', index=False
        )
        melted_relative_error_df.to_csv(
            f'{outdir}/{file_prefix}_relative_error_melted.csv', index=False
        )
        melted_est_obs_ratio_df.to_csv(
            f'{outdir}/{file_prefix}_est_obs_ratio_melted.csv', index=False
        )

        # Generate the Vega-Lite files
        generate_and_save_vega_lite_configs(outdir, group_var, file_prefix)



def generate_and_save_vega_lite_configs(outdir, group_var, file_prefix):
    output_dir = os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    metrics = ["percent_rmse", "relative_error", "est_obs_ratio"]
    metric_fields = ["Percent RMSE", "Relative Error", "Est/Obs Ratio"]
    file_suffixes = [
        "percent_rmse_melted.csv",
        "relative_error_melted.csv",
        "est_obs_ratio_melted.csv"]

    group_var_sort_orders = {
        'AT Group': [
            'All Locations',
            'Core/CBD',
            'UrbBiz',
            'Urb',
            'Sub'
        ],
        'FT Group': [
            'All Locations',
            'Fwy/Ramp',
            'Art',
            'Col',
            'Loc'
        ],
        'Observed Volume Category': [
            'All Locations',
            '<10k',
            '10-20k',
            '20-50k',
            '>=50k'
        ],
        'Loc Type': [
            'All Locations',
            'San Francisco',
            'SF Screenline',
            'Other County Screenline',
            'Other'
        ]
    }
    custom_order = group_var_sort_orders.get(group_var, None)

    for metric, y_field, file_suffix in zip(metrics, metric_fields, file_suffixes):
        file_path = os.path.join(output_dir, f"{file_prefix}{file_suffix}")

        # Adjust group_var for the naming step if it's 'Observed Volume Category'
        naming_group_var = "observedvolume" if group_var == 'Observed Volume Category' else group_var.lower().replace(' ', '')

        config = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {
                "url": f"{outdir}/{file_prefix}_{file_suffix}"
            },
            "mark": {
                "type": "bar",
                "stroke": "black",
                "cursor": "pointer",
                "tooltip": True
            },
            "encoding": {
                "x": {
                    "field": "Time Period",
                    "type": "nominal",
                    "sort": ["Daily", "AM", "MD", "PM", "EV", "EA"],
                    "axis": {"title": None, "labelAngle": -90}
                },
                "y": {"field": y_field, "type": "quantitative"},
                "detail": {"field": y_field},
                "xOffset": {
                    "field": group_var,
                    "type": "nominal",
                    "sort": custom_order,
                },
                "color": {
                    "field": group_var,
                    "type": "nominal",
                    "legend": {"title": "Category"},
                    "sort": custom_order,
                }
            }
        }
        config_file_path = os.path.join(
            output_dir, f"{outdir}/{naming_group_var}_{metric}.vega.json")
        try:
            with open(config_file_path, 'w') as f:
                json.dump(config, f, indent=4)
            print(f"Configuration for {metric} saved to: {config_file_path}")
        except Exception as e:
            print(f"Failed to save configuration for {metric}: {e}")
