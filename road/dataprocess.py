import os
import pandas as pd
from simpledbf import Dbf5

def generate_loaded_network_file_names(loaded_network_time_periods):
    """Generate a list of loaded_network file names based on time periods."""
    return [f"LOAD{tod}_FINAL.dbf" for tod in loaded_network_time_periods]

def filter_and_aggregate(
        obs_file,
        loaded_network_directory,
        loaded_network_files,
        column_names,
        time_periods,
        extra_columns,
        at_mapping,
        ft_mapping):
    loaded_network_files = generate_loaded_network_file_names(time_periods)

    # Read and process the Excel file    
    base_df = pd.read_csv(obs_file, usecols = extra_columns)

    # Initialize columns for time periods and daily total
    for col in time_periods + ['Daily']:
        base_df[col] = 0

    for loaded_network_file, output_column in zip(loaded_network_files, time_periods):
        loaded_network_file_path = os.path.join(loaded_network_directory, loaded_network_file)
        loaded_network = Dbf5(loaded_network_file_path)
        loaded_network_df = loaded_network.to_dataframe()
        loaded_networkf_df = loaded_network_df[column_names]

        # Create a dictionary for quick lookup of loaded_network data
        loaded_network_dict = {(row['A'], row['B']): row for index,
                    row in loaded_networkf_df.iterrows()}

        # Update the base dataframe with data from the loaded_network file
        for index, row in base_df.iterrows():
            key = (row['A'], row['B'])
            if key in loaded_network_dict:
                loaded_network_row = loaded_network_dict[key]

                for extra_col in column_names:
                    if extra_col not in ['A', 'B', 'V_1']:
                        base_df.at[index, extra_col] = loaded_network_row[extra_col]

                base_df.at[index, output_column] = loaded_network_row['V_1']

                base_df.at[index, 'Daily'] += loaded_network_row['V_1']

    # Map AT and FT values to their groups
    base_df['AT Group'] = base_df['AT'].map(at_mapping)
    base_df['FT Group'] = base_df['FT'].map(ft_mapping)

    return base_df