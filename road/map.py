import json
import pandas as pd
import geopandas as gpd
import numpy as np

def calculate_differences(est_df, obs_df, output):
    """
    Merges the estimated and observed dataframes and calculates various difference metrics.

    Args:
        est_df (pd.DataFrame): DataFrame containing estimated data with columns 'A', 'B', and time periods.
        obs_df (pd.DataFrame): DataFrame containing observed data with columns 'A', 'B', and time periods.
        output (str): Path to save the resulting CSV file.

    Returns:
        pd.DataFrame: Merged DataFrame with additional columns for ratio, difference, percentage difference,
                      absolute difference, and root mean squared error (RMSE) for each time period.
    """
    # Step 1: Handle NaN and Infinite Values in 'A' and 'B'
    est_df['A'] = pd.to_numeric(est_df['A'], errors='coerce')
    est_df['B'] = pd.to_numeric(est_df['B'], errors='coerce')
    obs_df['A'] = pd.to_numeric(obs_df['A'], errors='coerce')
    obs_df['B'] = pd.to_numeric(obs_df['B'], errors='coerce')

    # Drop rows with NaN or inf values in 'A' or 'B' in both DataFrames
    est_df = est_df.dropna(subset=['A', 'B'])
    obs_df = obs_df.dropna(subset=['A', 'B'])

    # Step 2: Convert columns 'A' and 'B' to integers, then to strings without decimal places
    est_df['A'] = est_df['A'].astype(int).astype(str)
    est_df['B'] = est_df['B'].astype(int).astype(str)
    obs_df['A'] = obs_df['A'].astype(int).astype(str)
    obs_df['B'] = obs_df['B'].astype(int).astype(str)

    # Step 3: Merge the estimated and observed dataframes on columns 'A' and 'B'
    time_periods = ['AM', 'MD', 'PM', 'EV', 'EA', 'Daily']
    merged_df = pd.merge(est_df, obs_df, on=['A', 'B'], suffixes=('_est', '_obs'))

    # Step 4: Calculate difference metrics for each time period
    for period in time_periods:
        est_col = f'{period}_est'
        obs_col = f'{period}_obs'
        merged_df[f'{period}_ratio'] = merged_df[est_col] / merged_df[obs_col]
        merged_df[f'{period}_diff'] = merged_df[est_col] - merged_df[obs_col]
        merged_df[f'{period}_pctdiff'] = merged_df[f'{period}_diff'] / merged_df[obs_col]
        merged_df[f'{period}_absdiff'] = abs(merged_df[f'{period}_diff'])
        merged_df[f'{period}_rmse'] = np.sqrt(merged_df[f'{period}_diff'] ** 2)

    # Step 5: Create 'AB' column for easy identification
    merged_df['AB'] = merged_df['A'] + ' ' + merged_df['B']

    # Step 6: Save the resulting DataFrame to a CSV file
    merged_df.to_csv(output, index=False)
    print(f"Results saved to {output}")

    return merged_df


def process_geospatial_data(merged_df, freeflow_path, output_path):
    """
    Reads a shapefile, merges it with the processed data, and outputs the result as a new shapefile.

    Args:
        merged_df (pd.DataFrame): DataFrame containing the merged data with calculated metrics.
        freeflow_path (str): Path to the shapefile containing freeflow data.
        output_path (str): Path to save the resulting shapefile.

    Returns:
        None
    """
    # Step 1: Read the shapefile into a GeoDataFrame
    gdf_freeflow = gpd.read_file(freeflow_path)
    print(f"Shapefile loaded with {len(gdf_freeflow)} rows.")


    # Step 2: Handle Coordinate Reference System (CRS)
    # Set the CRS if it's missing or incorrect
    if gdf_freeflow.crs is None or gdf_freeflow.crs.to_string() != 'epsg:2227':
        gdf_freeflow.crs = 'epsg:2227'  # Set CRS to the expected value
    # Convert to the desired CRS
    gdf_freeflow = gdf_freeflow.to_crs(epsg=4326)  # Use WGS 84 for geographic coordinates

    # Step 3: Clean and Convert Columns for Consistent Data Types
    # Replace 'nan' strings in merged_df with actual NaN values
    merged_df['A'] = merged_df['A'].replace('nan', np.nan)
    merged_df['B'] = merged_df['B'].replace('nan', np.nan)
    # Drop rows with NaN values in merged_df
    merged_df = merged_df.dropna(subset=['A', 'B'])

    # Convert to float, then to int, then to str to ensure clean merging
    merged_df['A'] = merged_df['A'].astype(float).astype(int).astype(str)
    merged_df['B'] = merged_df['B'].astype(float).astype(int).astype(str)

    # Step 4: Clean and Prepare gdf_freeflow for Merging
    gdf_freeflow['A'] = gdf_freeflow['A'].replace('nan', np.nan)
    gdf_freeflow['B'] = gdf_freeflow['B'].replace('nan', np.nan)
    gdf_freeflow = gdf_freeflow.dropna(subset=['A', 'B'])

    # Convert columns in gdf_freeflow to ensure they match merged_df types
    gdf_freeflow['A'] = gdf_freeflow['A'].astype(float).astype(int).astype(str)
    gdf_freeflow['B'] = gdf_freeflow['B'].astype(float).astype(int).astype(str)

    # Step 5: Create 'AB' Column for Merging Consistency
    gdf_freeflow['AB'] = gdf_freeflow['A'] + ' ' + gdf_freeflow['B']
    merged_df['AB'] = merged_df['A'] + ' ' + merged_df['B']

    # Step 6: Merge the GeoDataFrame with the processed DataFrame
    merged_gdf = gdf_freeflow[['A', 'B', 'AB', 'geometry']].merge(merged_df, on=['A', 'B', 'AB'], how='inner')
    print(f"Merged GeoDataFrame has {len(merged_gdf)} rows.")

    # Step 7: Ensure the result is a GeoDataFrame and save it as a new shapefile
    merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry='geometry')
    
    # Step 8: Save the merged GeoDataFrame to the specified output path
    merged_gdf.to_file(output_path, index=False)
    print(f"Shapefile saved to {output_path}.")


