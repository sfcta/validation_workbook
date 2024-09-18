import json
import pandas as pd
import geopandas as gpd
import numpy as np


def calculate_differences(est_df, obs_df,output):
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
    merged_df = pd.merge(
        est_df, obs_df, on=[
            'A', 'B'], suffixes=(
            '_est', '_obs'))

    for period in time_periods:
        est_col = f'{period}_est'
        obs_col = f'{period}_obs'
        merged_df[f'{period}_ratio'] = merged_df[est_col] / merged_df[obs_col]
        merged_df[f'{period}_diff'] = merged_df[est_col] - merged_df[obs_col]
        merged_df[f'{period}_pctdiff'] = (
            merged_df[f'{period}_diff']) / merged_df[obs_col]
        merged_df[f'{period}_absdiff'] = abs(merged_df[f'{period}_diff'])
        merged_df[f'{period}_rmse'] = np.sqrt(
            (merged_df[f'{period}_diff']) ** 2)

    merged_df['AB'] = merged_df['A'].astype(
        str) + ' ' + merged_df['B'].astype(str)
    merged_df.to_csv(output, index=False)
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
    # Read the shapefile into a GeoDataFrame
    gdf_freeflow = gpd.read_file(freeflow_path)
    gdf_freeflow.crs = 'epsg:2227'  # Set the coordinate reference system
    gdf_freeflow = gdf_freeflow.to_crs(epsg=4236)  # Convert to the desired CRS
    
    # Create the 'AB' column for merging
    gdf_freeflow['AB'] = gdf_freeflow['A'].astype(str) + ' ' + gdf_freeflow['B'].astype(str)
    
    # Merge the GeoDataFrame with the processed DataFrame
    merged_gdf = gdf_freeflow[['A', 'B', 'AB', 'geometry']].merge(merged_df, on=['A', 'B', 'AB'])
    
    # Ensure the result is a GeoDataFrame and save it as a new shapefile
    merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry='geometry')
    merged_gdf.to_file(output_path, index=False)


