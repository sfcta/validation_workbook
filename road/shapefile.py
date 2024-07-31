import pandas as pd
import geopandas as gpd
from io import StringIO
import json
import geopandas as gpd
import yaml
import os
import numpy as np


def calculate_differences(est_df, obs_df):
    """
    Merges the estimated and observed dataframes and calculates various difference metrics.

    Args:
        est_df (pd.DataFrame): DataFrame containing estimated data with columns 'A', 'B', and time periods.
        obs_df (pd.DataFrame): DataFrame containing observed data with columns 'A', 'B', and time periods.

    Returns:
        pd.DataFrame: Merged DataFrame with additional columns for ratio, difference, percentage difference,
                      absolute difference, and root mean squared error (RMSE) for each time period.
    """
    time_periods = ['AM', 'MD', 'PM', 'EV', 'EA', 'Daily']
    merged_df = pd.merge(est_df, obs_df, on=['A', 'B'], suffixes=('_est', '_obs'))

    for period in time_periods:
        est_col = f'{period}_est'
        obs_col = f'{period}_obs'
        merged_df[f'{period}_ratio'] = merged_df[est_col] / merged_df[obs_col]
        merged_df[f'{period}_diff'] = merged_df[est_col] - merged_df[obs_col]
        merged_df[f'{period}_pctdiff'] = (merged_df[f'{period}_diff']) / merged_df[obs_col]
        merged_df[f'{period}_absdiff'] = abs(merged_df[f'{period}_diff'])
        merged_df[f'{period}_rmse'] = np.sqrt((merged_df[f'{period}_diff']) ** 2)

    merged_df['AB'] = merged_df['A'].astype(str) + ' ' + merged_df['B'].astype(str)
    merged_df.to_csv('map_data.csv',index=False)
    return merged_df

def process_geospatial_data(merged_df, freeflow_path, output_path):
    """
    Reads a shapefile, merges it with the processed data, and outputs the result as a new shapefile.

    Args:
        merged_df (pd.DataFrame): DataFrame containing the merged data with calculated metrics.
        shapefile_path (str): Path to the shapefile containing freeflow data.
        output_path (str): Path to save the resulting shapefile.

    Returns:
        None
    """
    df_freeflow = gpd.read_file(freeflow_path)
    df_freeflow.crs = 'epsg:2227'
    df_freeflow = df_freeflow.to_crs(epsg=4236)
    df_freeflow['AB'] = df_freeflow['A'].astype(str) + ' ' + df_freeflow['B'].astype(str)

    me_df = pd.merge(merged_df, df_freeflow[['A', 'B', 'AB', 'geometry']], on=['A', 'B', 'AB'])
    me_df = gpd.GeoDataFrame(me_df, geometry='geometry')
    me_df.to_file(output_path, index=False)

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