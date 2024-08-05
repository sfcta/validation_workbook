import os
import pandas as pd
from simpledbf import Dbf5
import configparser
import json
import yaml
from sklearn.linear_model import LinearRegression
from io import StringIO
import geopandas as gpd
from dataprocess import *
from scatter import *
from stats import *
from map import *


config = configparser.ConfigParser()
config.read('config.ini')

# Extract the CHAMP input file names
dbf_directory = config['DBF']['DBF_Directory']
dbf_files_time = [dbf_file.strip()
             for dbf_file in config['DBF']['DBF_Files_TIME'].split(',')]
dbf_column_names = [name.strip() for name in config['DBF']
                    ['DBF_Column_Names'].split(',')]

# Extract the observed file name and tab
excel_file_path = config['OBSERVED FILE']['Excel_File_Path']
sheet_name = config['OBSERVED FILE']['Sheet_Name']
extra_columns = [col.strip() for col in config['OBSERVED FILE']
                 ['Excel_Extra_Columns'].split(',')]
obs_usecols = config['OBSERVED FILE']['Obs_usecols']


# Read the Obs data and the CHAMP estimation data
obs_df = pd.read_excel(
    excel_file_path,
    skiprows=1,
    sheet_name=sheet_name,
    usecols=obs_usecols)
obs_df = obs_df.drop_duplicates(subset=['A', 'B'])
est_df = filter_and_aggregate(
    excel_file_path,
    sheet_name,
    dbf_directory,
    dbf_files,
    dbf_column_names,
    dbf_files_time,
    extra_columns)


# Part 1
# Scatter variables
chosen_timeperiod = config['SCATTER INPUT']['Chosen_period']
classification_col = config['SCATTER INPUT']['Classification_col']
combined_df_cols = [col.strip() for col in config['SCATTER INPUT']
                    ['Combined_DF_Cols'].split(',')]
# Est-Obs Plot
fields1 = config['EST SCATTER PLOT']['Fields'].split(', ')
nominal_fields1 = config['EST SCATTER PLOT']['NominalFields'].split(', ')
x_field1 = config['EST SCATTER PLOT']['XField']
y_field1 = config['EST SCATTER PLOT']['YField']
name1 = config['EST SCATTER PLOT']['Name']
# Percentile Plot
fields2 = config['PERCENT SCATTER PLOT']['Fields'].split(', ')
nominal_fields2 = config['PERCENT SCATTER PLOT']['NominalFields'].split(', ')
x_field2 = config['PERCENT SCATTER PLOT']['XField']
y_field2 = config['PERCENT SCATTER PLOT']['YField']
name2 = config['PERCENT SCATTER PLOT']['Name']
# Dashborad num
dashboard_num = config['SCATTER YAML']['Dashboard_number']

# Part 2
combined_df_cols = [col.strip() for col in config['STATS INPUT']
                    ['Combined_DF_Cols'].split(',')]
group_vars = ['Observed Volume', 'Loc Type', 'AT Grp', 'FT Grp']
times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']

# Part 3
freeflow_path = config['MAP INPUT']['Freeflow_Dir']
shp_output_path = config['MAP INPUT']['Shp_out_Di']

data_file = config['MAP YAML']['Csv_file']
shapes_file = config['MAP YAML']['Shape_file']
join_field = config['MAP YAML']['Join']
line_width_col = config['MAP YAML']['Line_wid_col']
line_color_col = config['MAP YAML']['Line_color_col']
breakpoints = config['MAP YAML']['breakpoints']
dashboard_num = config['MAP YAML']['dashboard_num']
center = config['MAP YAML']['Center']


# Part1 - Scatter Plot
# calculate the metrics
Select_time_period_loc_df, classification_col_types, file_name = compute_and_save_errors(
    est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col)

# Generate the Scatter plots required files
row_count = 1
for types in classification_col_types:
    # Generate est vs obs scatter polt
    generate_vega_lite_json_est(
        file_name,
        classification_col,
        types,
        x_field1,
        y_field1,
        fields1,
        nominal_fields1,
        name1)
    # Generate percent errors vs obs scatter polt
    generate_vega_lite_json_diffpercent(
        file_name,
        classification_col,
        types,
        x_field2,
        y_field2,
        fields2,
        nominal_fields2,
        name2)

    yaml_config['layout'][f'row{row_count}'] = generate_yaml_config(
        types, name1, name2)
    row_count += 1

generate_vega_lite_json_est(
    file_name,
    classification_col,
    'all',
    x_field1,
    y_field1,
    fields1,
    nominal_fields1,
    name1,
    include_all_data=True)

generate_vega_lite_json_diffpercent(
    file_name,
    classification_col,
    'all',
    x_field2,
    y_field2,
    fields2,
    nominal_fields2,
    name2,
    include_all_data=True)

yaml_config['layout'][f'row{row_count}'] = generate_yaml_config(
    'all', name1, name2, include_all_data=True)
yaml_file_path = f"dashboard{dashboard_num}-Scatter.yaml"
with open(yaml_file_path, 'w') as file:
    yaml.dump(yaml_config, file, default_flow_style=False)


# Part 2 - Validation Stats
time_period_dfs = prepare_time_period_dfs(
    est_df, obs_df, times, combined_df_cols)
generate_and_save_tables(time_period_dfs, group_vars)


# Part 3 - Map
merged_df = calculate_differences(est_df, obs_df)
process_geospatial_data(merged_df, freeflow_path, shp_output_path)
create_yaml_file(
    center,
    shapes_file,
    data_file,
    join_field,
    line_width_col,
    line_color_col,
    breakpoints,
    dashboard_num)
