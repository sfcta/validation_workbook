import os
import pandas as pd
import numpy as np
from simpledbf import Dbf5
import configparser
import json
import yaml

def handle_merged_header(excel_file, sheet_name, extra_columns):
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, skiprows=1)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    return df[extra_columns]

def filter_and_aggregate(excel_file, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns):
    excel_df = handle_merged_header(excel_file, sheet_name, extra_columns)
    base_df = excel_df.reset_index(drop=True)

    for col in output_columns + ['Daily']:
        base_df[col] = 0

    for dbf_file, output_column in zip(dbf_files, output_columns):
        dbf_file_path = f"{dbf_directory}\{dbf_file}"
        dbf = Dbf5(dbf_file_path)
        dbf_df = dbf.to_dataframe()
        dbf_df = dbf_df[column_names]

        dbf_dict = {(row['A'], row['B']): row for index, row in dbf_df.iterrows()}

        for index, row in base_df.iterrows():
            key = (row['A'], row['B'])
            if key in dbf_dict:
                dbf_row = dbf_dict[key]

                for extra_col in column_names:
                    if extra_col not in ['A', 'B', 'V_1']:
                        base_df.at[index, extra_col] = dbf_row[extra_col]

                base_df.at[index, output_column] = dbf_row['V_1']

                base_df.at[index, 'Daily'] += dbf_row['V_1']

    return base_df
os.chdir(os.path.dirname(os.path.abspath(__file__)))
config = configparser.ConfigParser()
config.read('config_template.ini')

dbf_directory = config['DEFAULT']['DBF_Directory']
dbf_files = [dbf_file.strip() for dbf_file in config['DEFAULT']['DBF_Files'].split(',')]
excel_file_path = config['DEFAULT']['Excel_File_Path']
sheet_name = config['DEFAULT']['Sheet_Name']
column_names = [name.strip() for name in config['DEFAULT']['DBF_Column_Names'].split(',')]
output_columns = ['AM', 'MD', 'PM', 'EV', 'EA']
extra_columns = [col.strip() for col in config['DEFAULT']['Excel_Extra_Columns'].split(',')]
obs_usecols = config['DEFAULT']['Obs_usecols']
chosen_timeperoid = config['DEFAULT']['Chosen_period']

combined_df_cols = [col.strip() for col in config['DEFAULT']['Combined_DF_Cols'].split(',')]
group_var = config['DEFAULT']['Group_var']


if len(dbf_files) != len(output_columns):
    raise ValueError("The number of DBF files and output column names do not match.")

est_df = filter_and_aggregate(excel_file_path, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns)

obs_df = pd.read_excel(excel_file_path, skiprows=1, sheet_name=sheet_name, usecols=obs_usecols)
obs_df = obs_df.drop_duplicates(subset=['A', 'B'])
obs_data = obs_df.iloc[:,-5:]
obs_data['Daily'] = obs_data.sum(axis=1)
common_columns = obs_data.columns

error = {}
for col in common_columns:
    error[col] = est_df[col] - obs_data[col]
error_df = pd.DataFrame(error)
error_squared_df = error_df.pow(2)
error_percent_df = error_df.divide(obs_data).fillna(0)
error_percent_df = error_percent_df.apply(lambda x: x * 100, axis=1)

combined_df = pd.concat([est_df, obs_data, error_df, error_squared_df, error_percent_df], axis=1)
combined_df = combined_df.drop_duplicates(subset=['A', 'B'], keep='first')
time_periods = chosen_timeperoid.split(', ')

# Initializing the dictionary to hold DataFrames for each time period
time_period_dfs = {}

# Predefined columns to be used in the calculations
calculation_cols = ['Estimated Volume', 'Observed Volume', 'Errors', 'Squared Errors', 'Percent Errors']

# Iterate through each time period and perform operations
for period in time_periods:
    # Filter columns that match the current time period by their occurrence
    matching_column_indices = [i for i, col_name in enumerate(combined_df.columns) if col_name == period]
    Select_time_period_df = combined_df.iloc[:, matching_column_indices]

    # Assuming combined_df_cols are the specific columns you want to retain from combined_df
    specific_columns_combined_df = combined_df[combined_df_cols]

    # Concatenate the specific columns with the selected time period columns
    Select_time_period_loc_df = pd.concat([specific_columns_combined_df, Select_time_period_df], axis=1)

    # Rename columns in the concatenated DataFrame
    new_column_names = combined_df_cols + calculation_cols[:len(Select_time_period_loc_df.columns) - len(combined_df_cols)]
    Select_time_period_loc_df.columns = new_column_names

    # Store the resulting DataFrame in the dictionary
    time_period_dfs[period] = Select_time_period_loc_df

area_type_mapping = {
    0: 'Core/CBD',
    1: 'Core/CBD',
    2: 'UrbBiz',
    3: 'Urb',
    4: 'Sub',
    5: 'Sub'
}
facility_type_mapping ={
    1: 'Fwy/Ramp',
    2: 'Fwy/Ramp',
    3: 'Fwy/Ramp',
    4: 'Col',
    5: 'Fwy/Ramp',
    6: 'TBD',
    7: 'Art',
    8: 'TBD',
    9: 'Loc',
    10: 'TBD',
    11: 'Loc',
    12: 'Art',
    13: 'TBD',
    14: 'TBD',
    15: 'Art'
}

def classify_observation_volume(volume):
    if volume < 10000:
        return '<10k'
    elif 10000 <= volume < 20000:
        return '10-20k'
    elif 20000 <= volume < 50000:
        return '20-50k'
    else:
        return '>=50k'
percent_rmse_df = pd.DataFrame()
relative_error_df = pd.DataFrame()
est_obs_ratio_df = pd.DataFrame()

for period, df in time_period_dfs.items():
    if group_var == 'AT':
        df['AT_Category'] = df['AT'].map(area_type_mapping)
        group_var_column = 'AT_Category'
    elif group_var == 'FT':
        df['FT_Category'] = df['FT'].map(facility_type_mapping)
        group_var_column = 'FT_Category'
    elif group_var == 'Observed Volume':
        df['Observed Volume Category'] = df['Observed Volume'].apply(classify_observation_volume)
        group_var_column = 'Observed Volume Category'
    else:
        group_var_column = group_var
    sum_estimated = df.groupby(group_var_column)['Estimated Volume'].sum()
    sum_observed = df.groupby(group_var_column)['Observed Volume'].sum()
    
    group = df.groupby(group_var_column).agg(
        sum_squared_errors=('Squared Errors', 'sum'),
        sum_observed=('Observed Volume', 'sum'),
        count=('Loc Type', 'count')
    )
    
    group['RMSE'] = np.sqrt(group['sum_squared_errors'] / group['count']) / (group['sum_observed'] / group['count'])
    group['percent_rmse'] = group['RMSE'] * 100
    percent_rmse_df[period] = group['percent_rmse']

    relative_error = (sum_estimated - sum_observed) / sum_observed
    relative_error_df[period] = relative_error
    
    est_obs_ratio = sum_estimated / sum_observed * 100
    est_obs_ratio_df[period] = est_obs_ratio

    total_sum_squared_errors = df['Squared Errors'].sum()
    total_sum_observed = df['Observed Volume'].sum()
    total_count = df.shape[0]
    
    total_rmse = np.sqrt(total_sum_squared_errors / total_count) / (total_sum_observed / total_count)
    total_percent_rmse = total_rmse * 100
    percent_rmse_df.loc['All Locations', period] = total_percent_rmse

    total_sum_estimated = df['Estimated Volume'].sum()
    total_relative_error = (total_sum_estimated - total_sum_observed) / total_sum_observed
    relative_error_df.loc['All Locations', period] = total_relative_error
    
    total_est_obs_ratio = total_sum_estimated / total_sum_observed
    est_obs_ratio_df.loc['All Locations', period] = total_est_obs_ratio

def reset_index_and_rename(df, group_var):
    # Reset the index to make the current index a column (if it's not already a column)
    df.index.name = group_var
    # Reset the index, which promotes the index to a column
    df = df.reset_index()
    return df

# Applying the reset and rename function to each DataFrame
percent_rmse_df = reset_index_and_rename(percent_rmse_df, group_var)
relative_error_df = reset_index_and_rename(relative_error_df, group_var)
est_obs_ratio_df = reset_index_and_rename(est_obs_ratio_df, group_var)

if group_var == 'Observed Volume':

    obs_df
    obs_df['Observed Volume'] = obs_df['Daily'].apply(classify_observation_volume)

    counts = obs_df['Observed Volume'].value_counts()
    count_df = pd.DataFrame(counts).reset_index()
    count_df.columns = ['Observed Volume', 'Count']
else:
    counts = combined_df[group_var].value_counts()
    count_df = pd.DataFrame(counts).reset_index()
    count_df.columns = [group_var, 'Count']

    if group_var == 'AT':
        count_df[group_var] = count_df[group_var].map(area_type_mapping).fillna('Other')
    elif group_var == 'FT':
        count_df[group_var] = count_df[group_var].map(facility_type_mapping).fillna('Other')

count_df = count_df.groupby(group_var, as_index=False)['Count'].sum()

total_count = count_df['Count'].sum()
total_row = pd.DataFrame({group_var: ['All Locations'], 'Count': [total_count]})

count_df = pd.concat([count_df, total_row], ignore_index=True)

area_type_order = ['Core/CBD', 'UrbBiz', 'Urb', 'Sub', 'All Locations']
facility_type_order = ['Fwy/Ramp', 'Art', 'Col', 'Loc', 'All Locations']
volume_classification_order = ['<10k', '10-20k', '20-50k', '>=50k', 'All Locations']

# Function to reorder DataFrame based on group_var
def reorder_dataframe(df, group_var):
    if group_var == 'AT':
        order = area_type_order
    elif group_var == 'FT':
        order = facility_type_order
    elif group_var == 'Observed Volume':
        order = volume_classification_order
    else:
        return df  # No reordering needed for other group_var values

    # Ensure "All Locations" is at the end
    df = df.set_index(group_var)
    df = df.reindex(order)
    df = df.reset_index()

    return df

# Apply the reordering to your DataFrames
percent_rmse_df = reorder_dataframe(percent_rmse_df, group_var)
relative_error_df = reorder_dataframe(relative_error_df, group_var)
est_obs_ratio_df = reorder_dataframe(est_obs_ratio_df, group_var)
count_df = reorder_dataframe(count_df, group_var)

percent_rmse_df = percent_rmse_df.round(1)
relative_error_df = relative_error_df.round(5)
est_obs_ratio_df = est_obs_ratio_df.round(3)

group_var_no_spaces = group_var.replace(" ", "").lower()
file_prefix = f"{group_var_no_spaces}_"
percent_rmse_df.to_csv(f'{file_prefix}percent_rmse.csv', index=False)
relative_error_df.to_csv(f'{file_prefix}relative_error.csv', index=False)
est_obs_ratio_df.to_csv(f'{file_prefix}est_obs_ratio.csv', index=False)
count_df.to_csv(f'{file_prefix}count.csv', index=False)

melted_percent_rmse_df = percent_rmse_df.melt(id_vars=[group_var], var_name='Time Period', value_name='Percent RMSE')
melted_relative_error_df = relative_error_df.melt(id_vars=[group_var], var_name='Time Period', value_name='Relative Error')
melted_est_obs_ratio_df = est_obs_ratio_df.melt(id_vars=[group_var], var_name='Time Period', value_name='Est/Obs Ratio')

melted_percent_rmse_df.to_csv(f'{file_prefix}percent_rmse_melted.csv', index=False)
melted_relative_error_df.to_csv(f'{file_prefix}relative_error_melted.csv', index=False)
melted_est_obs_ratio_df.to_csv(f'{file_prefix}est_obs_ratio_melted.csv', index=False)

# Function to generate Vega-Lite configuration files
def generate_and_save_vega_lite_configs(group_var, file_prefix):
    # Define the directory to save the Vega-Lite configuration files
    output_dir = "X:/Projects/Miscellaneous/validation_simwrapper/roads/rmse"
    os.makedirs(output_dir, exist_ok=True)

    # Mapping group_var to the corresponding field in the visu
    # Configurations for different metrics
    metrics = ["percent_rmse", "relative_error", "est_obs_ratio"]
    metric_fields = ["Percent RMSE", "Relative Error", "Est/Obs Ratio"]
    file_suffixes = ["percent_rmse_melted.csv", "relative_error_melted.csv", "est_obs_ratio_melted.csv"]

    group_var_sort_orders = {
        'AT': ['Core/CBD', 'UrbBiz', 'Urb', 'Sub', 'All Locations'],
        'FT': ['Fwy/Ramp', 'Art', 'Col', 'Loc', 'All Locations'],
        'Volume': ['<10k', '10-20k', '20-50k', '>=50k', 'All Locations'],
        # Add other mappings for different values of group_var if necessary
    }
    custom_order = group_var_sort_orders.get(group_var, None)
    
    for metric, y_field, file_suffix in zip(metrics, metric_fields, file_suffixes):
        file_path = f"rmse/{file_prefix}{file_suffix}"
        config = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {
                "url": file_path
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
                    # Use the custom order based on the current value of group_var
                    "sort": custom_order,
                },
                "color": {
                    "condition": {
                        "test": f"datum['{group_var}'] == 'All Locations'",
                        "value": "black"  # Set the color to black if the condition is true
                    },
                    "field": group_var,
                    "type": "nominal",
                    "legend": {"title": "Category"},
                    "sort": custom_order,
                    # Optionally, specify a scale to define colors for other categories if necessary
                }
            }
        }
        # Save the configuration to a JSON file
        config_file_path = os.path.join(output_dir, f"{group_var.lower().replace(' ', '')}_{metric}.vega.json")
        with open(config_file_path, 'w') as f:
            json.dump(config, f, indent=4)

        print(f"Configuration for {metric} saved to: {config_file_path}")

# Generate and save Vega-Lite configurations
generate_and_save_vega_lite_configs(group_var, file_prefix)

