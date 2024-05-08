import pandas as pd
import geopandas as gpd
from io import StringIO
import json
import geopandas as gpd
from simpledbf import Dbf5
import configparser
import yaml
import os
import numpy as np

config = configparser.ConfigParser()
config.read('X:/Projects/Miscellaneous/validation_simwrapper/roads/map/map_config.ini')

dbf_directory = config['DEFAULT']['DBF_Directory']
dbf_files = [dbf_file.strip() for dbf_file in config['DEFAULT']['DBF_Files'].split(',')]
excel_file_path = config['DEFAULT']['Excel_File_Path']
sheet_name = config['DEFAULT']['Sheet_Name']
column_names = [name.strip() for name in config['DEFAULT']['DBF_Column_Names'].split(',')]
output_columns = ['AM', 'MD', 'PM', 'EV', 'EA']
extra_columns = [col.strip() for col in config['DEFAULT']['Excel_Extra_Columns'].split(',')]
obs_usecols = config['DEFAULT']['Obs_usecols']


data_file = config['YAML']['Csv_file']
shapes_file = config['YAML']['Shape_file']
join_field =  config['YAML']['Join']
line_width_col = config['YAML']['Line_wid_col']
line_color_col = config['YAML']['Line_color_col']
breakpoints = config['YAML']['breakpoints']
dashboard_num = config['YAML']['dashboard_num']
center = config['YAML']['Center']

def handle_merged_header(excel_file, sheet_name, extra_columns):
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, skiprows=1)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    return df[extra_columns]

def filter_and_aggregate(excel_file, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns):
    excel_df = handle_merged_header(excel_file, sheet_name, extra_columns)
    base_df = excel_df.reset_index(drop=True)

    # Initialize columns in base_df for each time period and 'Daily'
    for col in output_columns + ['Daily']:
        base_df[col] = 0

    for dbf_file, output_column in zip(dbf_files, output_columns):
        dbf_file_path = f"{dbf_directory}\{dbf_file}"
        dbf = Dbf5(dbf_file_path)
        dbf_df = dbf.to_dataframe()
        dbf_df = dbf_df[column_names]

        # Create a dictionary for fast lookup using 'A' and 'B' as the matching columns
        dbf_dict = {(row['A'], row['B']): row for index, row in dbf_df.iterrows()}

        # Loop through base_df and fetch data based on matching columns 'A' and 'B'
        for index, row in base_df.iterrows():
            key = (row['A'], row['B'])
            if key in dbf_dict:
                dbf_row = dbf_dict[key]

                # Fetch values for the selected columns from DBF and add them to base_df
                # Skip 'V_1' if it's not needed as a separate column
                for extra_col in column_names:  # Use all columns from DBF
                    if extra_col not in ['A', 'B', 'V_1']:  # Skip 'A', 'B', and 'V_1' if not needed as separate columns
                        base_df.at[index, extra_col] = dbf_row[extra_col]

                # Update the time period column with 'V_1' value
                base_df.at[index, output_column] = dbf_row['V_1']

                # Add the 'V_1' value to the 'Daily' column
                base_df.at[index, 'Daily'] += dbf_row['V_1']

    return base_df

est_df = filter_and_aggregate(excel_file_path, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns)
obs_df = pd.read_excel(excel_file_path, skiprows=1, sheet_name=sheet_name, usecols=obs_usecols)
obs_df = obs_df.drop_duplicates(subset=['A', 'B'])
merged_df = pd.merge(est_df, obs_df, on=['A','B'], suffixes=('_est', '_obs'))
time_periods = ['AM', 'MD', 'PM', 'EV', 'EA','Daily']

# Initialize columns for squared errors and percent errors
for period in time_periods:
    est_col = f'{period}_est'
    obs_col = f'{period}_obs'
    diff = f'{period}_diff'
    pct_diff = f'{period}_pctdiff'
    abs_diff = f'{period}_absdiff'
    ratio_col = f'{period}_ratio'
    rmse = f'{period}_rmse'
    merged_df[ratio_col] = merged_df[est_col] / merged_df[obs_col]
    merged_df[diff] = merged_df[est_col] - merged_df[obs_col]
    merged_df[pct_diff] = (merged_df[est_col] - merged_df[obs_col])/merged_df[obs_col]
    merged_df[abs_diff] = abs(merged_df[est_col] - merged_df[obs_col])
    merged_df[rmse] = np.sqrt(((merged_df[est_col]-merged_df[obs_col])**2))
merged_df['AB'] = merged_df['A'].astype(str) + ' ' + merged_df['B'].astype(str)
merged_df.to_csv('map_data.csv',index=False)

df_freeflow = gpd.read_file('X:/Projects/DTX/CaltrainValidation/s8_2019_Base/freeflow.shp')
df_freeflow.crs = 'epsg:2227'
df_freeflow = df_freeflow.to_crs(epsg=4236)
df_freeflow = df_freeflow[['A','B','AB','geometry']]

me_df = pd.merge(merged_df,df_freeflow,on=['A','B','AB'])
me_df = gpd.GeoDataFrame(me_df, geometry='geometry')
me_df.to_file('road.shp',index=False)

def create_yaml_file(center, shapes_file, data_file, join_field, line_width_col, line_color_col, breakpoints, dashboard_num):

    yaml_content = {
        'header': {
            'title': 
            'description' 
        },
        'layout': {
            'row1': [
                {
                    'type': 'map',
                    'title': 'Vis_map',
                    'description': '',
                    'height': 15,
                    'center': center,
                    'zoom': 10,                    'shapes': shapes_file,
                    'datasets': {
                        'data': {
                            'file': data_file,
                            'join': join_field
                        }
                    },
                    'display': {
                        'lineWidth': {
                            'dataset': 'data',
                            'columnName': line_width_col,
                            'scaleFactor': 200
                        },
                        'lineColor': {
                            'dataset': 'data',
                            'columnName': line_color_col,
                            'colorRamp': {
                                'ramp': 'PRGn',
                                'steps': len(breakpoints)+1,
                                'breakpoints': breakpoints
                            }
                        }
                    }
                }
            ]
        }
    }

    yaml_file_path = f"dashboard{dashboard_num}-map.yaml"
    with open(yaml_file_path, 'w') as file:
        yaml.dump(yaml_content, file, default_flow_style=False)

create_yaml_file(center, shapes_file, data_file, join_field, line_width_col, line_color_col, breakpoints, dashboard_num)