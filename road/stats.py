import os
import pandas as pd
import numpy as np
import json
from road.validation_road_utils import compute_and_combine_stats


def prepare_time_period_dfs(est_df, obs_df, times, combined_df_cols):
    # Call the compute_and_combine function to get the combined dataframe
    combined_df = compute_and_combine_stats(est_df, obs_df, times, combined_df_cols)
    
    # Remove duplicates
    combined_df = combined_df.drop_duplicates(subset=['A', 'B'], keep='first')

    calculation_cols = [
        'Estimated Volume',
        'Observed Volume',
        'Errors',
        'Squared Errors',
        'Percent Errors']

    time_period_dfs = {}

    for period in times:
        matching_column_indices = [
            i for i, col_name in enumerate(
                combined_df.columns) if period in col_name]
        select_time_period_df = combined_df.iloc[:, matching_column_indices]

        specific_columns_combined_df = combined_df[combined_df_cols]

        select_time_period_loc_df = pd.concat(
            [specific_columns_combined_df, select_time_period_df], axis=1)

        new_column_names = combined_df_cols + \
            calculation_cols[:len(select_time_period_loc_df.columns) - len(combined_df_cols)]
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
    sum_estimated = df.groupby(group_var_column)['Estimated Volume'].sum()
    sum_observed = df.groupby(group_var_column)['Observed Volume'].sum()

    group = df.groupby(group_var_column).agg(
        sum_squared_errors=('Squared Errors', 'sum'),
        sum_observed=('Observed Volume', 'sum'),
        count=('Loc Type', 'count')
    )

    group['RMSE'] = np.sqrt(group['sum_squared_errors'] / \
                            group['count']) / (group['sum_observed'] / group['count'])
    group['percent_rmse'] = group['RMSE'] * 100

    relative_error = (sum_estimated - sum_observed) / sum_observed
    est_obs_ratio = sum_estimated / sum_observed * 100

    total_sum_squared_errors = df['Squared Errors'].sum()
    total_sum_observed = df['Observed Volume'].sum()
    total_count = df.shape[0]

    total_rmse = np.sqrt(total_sum_squared_errors /
                         total_count) / (total_sum_observed / total_count)
    total_percent_rmse = total_rmse * 100

    total_sum_estimated = df['Estimated Volume'].sum()
    total_relative_error = (
        total_sum_estimated - total_sum_observed) / total_sum_observed
    total_est_obs_ratio = total_sum_estimated / total_sum_observed

    return group['percent_rmse'], relative_error, est_obs_ratio, total_percent_rmse, total_relative_error, total_est_obs_ratio


def generate_and_save_tables(outdir, time_period_dfs, group_vars):
    # Create the stats_data directory if it does not exist

    for group_var in group_vars:
        percent_rmse_df = pd.DataFrame()
        relative_error_df = pd.DataFrame()
        est_obs_ratio_df = pd.DataFrame()

        for period, df in time_period_dfs.items():
            if group_var == 'Observed Volume':
                df['Observed Volume Category'] = df['Observed Volume'].apply(
                    classify_observation_volume)
                group_var_column = 'Observed Volume Category'
            else:
                group_var_column = group_var

            percent_rmse, relative_error, est_obs_ratio, total_percent_rmse, total_relative_error, total_est_obs_ratio = calculate_metrics(
                df, group_var_column, period)

            percent_rmse_df[period] = percent_rmse
            relative_error_df[period] = relative_error
            est_obs_ratio_df[period] = est_obs_ratio

            percent_rmse_df.loc['All Locations', period] = total_percent_rmse
            relative_error_df.loc['All Locations',
                                  period] = total_relative_error
            est_obs_ratio_df.loc['All Locations', period] = total_est_obs_ratio

        # Reset index and rename columns
        percent_rmse_df = reset_index_and_rename(percent_rmse_df, group_var)
        relative_error_df = reset_index_and_rename(
            relative_error_df, group_var)
        est_obs_ratio_df = reset_index_and_rename(est_obs_ratio_df, group_var)

        # Reorder DataFrame based on group_var
        percent_rmse_df = reorder_dataframe(percent_rmse_df, group_var)
        relative_error_df = reorder_dataframe(relative_error_df, group_var)
        est_obs_ratio_df = reorder_dataframe(est_obs_ratio_df, group_var)

        # Count for each group_var category
        if group_var == 'Observed Volume':
            df['Observed Volume Category'] = df['Observed Volume'].apply(
                classify_observation_volume)
            counts = df['Observed Volume Category'].value_counts()
            count_df = pd.DataFrame(counts).reset_index()
            count_df.columns = ['Observed Volume', 'Count']
        else:
            counts = df[group_var].value_counts()
            count_df = pd.DataFrame(counts).reset_index()
            count_df.columns = [group_var, 'Count']

        # Handle NaN values
        if group_var in ['AT Group', 'FT Group']:
            count_df[group_var] = count_df[group_var].fillna('Other')

        count_df = count_df.groupby(group_var, as_index=False)['Count'].sum()

        total_count = count_df['Count'].sum()
        total_row = pd.DataFrame(
            {group_var: ['All Locations'], 'Count': [total_count]})
        count_df = pd.concat([count_df, total_row], ignore_index=True)
        count_df = reorder_dataframe(count_df, group_var)

        # Round the dataframes for better readability
        percent_rmse_df = percent_rmse_df.round(1)
        relative_error_df = relative_error_df.round(5)
        est_obs_ratio_df = est_obs_ratio_df.round(3)

        # Save the dataframes to CSV
        group_var_no_spaces = group_var.replace(" ", "").lower()
        file_prefix = f"{group_var_no_spaces}_"
        count_df.to_csv(f'{outdir}/{file_prefix}count.csv', index=False)
        percent_rmse_df.to_csv(f'{outdir}/percent_rmse_{group_var_no_spaces}.csv')
        relative_error_df.to_csv(f'{outdir}/relative_error_{group_var_no_spaces}.csv')
        est_obs_ratio_df.to_csv(f'{outdir}/est_obs_ratio_{group_var_no_spaces}.csv')

        # Melt dataframes for easier plotting or analysis
        melted_percent_rmse_df = percent_rmse_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Percent RMSE')
        melted_relative_error_df = relative_error_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Relative Error')
        melted_est_obs_ratio_df = est_obs_ratio_df.melt(
            id_vars=[group_var], var_name='Time Period', value_name='Est/Obs Ratio')

        # Save melted dataframes to CSV
        melted_percent_rmse_df.to_csv(
            f'{outdir}/{file_prefix}percent_rmse_melted.csv', index=False)
        melted_relative_error_df.to_csv(
            f'{outdir}/{file_prefix}relative_error_melted.csv', index=False)
        melted_est_obs_ratio_df.to_csv(
            f'{outdir}/{file_prefix}est_obs_ratio_melted.csv', index=False)

        # generate the vega-lite files
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
            'Core/CBD',
            'UrbBiz',
            'Urb',
            'Sub',
            'All Locations'],
        'FT Group': [
            'Fwy/Ramp',
            'Art',
            'Col',
            'Loc',
            'All Locations'],
        'Observed Volume': [
            '<10k',
            '10-20k',
            '20-50k',
            '>=50k',
            'All Locations'],
    }
    custom_order = group_var_sort_orders.get(group_var, None)

    for metric, y_field, file_suffix in zip(
            metrics, metric_fields, file_suffixes):
        file_path = os.path.join(output_dir, f"{file_prefix}{file_suffix}")

        config = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {
                "url": f"{outdir}/{file_prefix}{file_suffix}"
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
                    "condition": {
                        "test": f"datum['{group_var}'] == 'All Locations'",
                        "value": "black"
                    },
                    "field": group_var,
                    "type": "nominal",
                    "legend": {"title": "Category"},
                    "sort": custom_order,
                }
            }
        }
        config_file_path = os.path.join(
            output_dir, f"{outdir}/{group_var.lower().replace(' ', '')}_{metric}.vega.json")
        try:
            with open(config_file_path, 'w') as f:
                json.dump(config, f, indent=4)
            print(f"Configuration for {metric} saved to: {config_file_path}")
        except Exception as e:
            print(f"Failed to save configuration for {metric}: {e}")