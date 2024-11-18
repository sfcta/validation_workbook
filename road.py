import toml
import string
import pandas as pd
import argparse
import sys, os
from dataprocess import generate_loaded_network_file_names, filter_and_aggregate
from scatter import compute_and_save_errors, generate_vega_lite_json_est, generate_vega_lite_json_diffpercent
from stats import prepare_time_period_dfs, generate_and_save_tables
from map import calculate_differences, process_geospatial_data

def csv_col_letter_to_num(letter):
    num = 0
    for char in letter:
        if char in string.ascii_letters:
            num = num * 26 + (ord(char.upper()) - ord('A')) + 1
    return num - 1


def scatter_plot_output(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                        fields1, nominal_fields1, x_field1, y_field1, name1, 
                        fields2, nominal_fields2, x_field2, y_field2, name2,
                        vega_est_output_path, vega_diffpercent_output_path):

    # Calculate the metrics
    select_time_period_loc_df, classification_col_types, file_name = compute_and_save_errors(
        est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name)

    # Generate the Scatter plots required files
    for types in classification_col_types:
        # Generate est vs obs scatter plot
        generate_vega_lite_json_est(
            file_name,
            classification_col,
            types,
            x_field1,
            y_field1,
            fields1,
            nominal_fields1,
            name1,
            vega_est_output_path)
        # Generate percent errors vs obs scatter plot
        generate_vega_lite_json_diffpercent(
            file_name,
            classification_col,
            types,
            x_field2,
            y_field2,
            fields2,
            nominal_fields2,
            name2,
            vega_diffpercent_output_path)

    # Generate scatter plots for all data
    generate_vega_lite_json_est(
        file_name,
        classification_col,
        'all',
        x_field1,
        y_field1,
        fields1,
        nominal_fields1,
        name1,
        vega_est_output_path,
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
        vega_diffpercent_output_path,
        include_all_data=True,)



def scatter_plot(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                 fields1, nominal_fields1, x_field1, y_field1, name1, 
                 fields2, nominal_fields2, x_field2, y_field2, name2,
                 vega_est_output_path, vega_diffpercent_output_path):
    # This function encapsulates the scatter plot generation logic
    scatter_plot_output(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                        fields1, nominal_fields1, x_field1, y_field1, name1, 
                        fields2, nominal_fields2, x_field2, y_field2, name2,
                        vega_est_output_path, vega_diffpercent_output_path)
    return


def validation_road(config):
    # Output directory
    outdir = config['OUTPUT']['directory']
    
    # Extract the CHAMP input file names
    loaded_network_directory = config['LOADED_NETWORK']['path']
    loaded_network_files_time = config['LOADED_NETWORK']['timeperiods']
    loaded_network_column_names = config['LOADED_NETWORK']['columns']
    
    # Generate loaded_network file names
    loaded_network_files = generate_loaded_network_file_names(loaded_network_files_time)

    # Load mappings from the config file
    at_mapping = config['AT']['values']
    ft_mapping = config['FT']['values']
    # Assuming 'AT' and 'FT' contain integer keys or some keys you can map
    at_mapping_dict = {i: at_mapping[i] for i in range(len(at_mapping))}
    ft_mapping_dict = {i: ft_mapping[i] for i in range(len(ft_mapping))}

    # Extract the observed file name and tab
    obs_filepath = config['OBSERVED_COUNTS']['obs_filepath']
    extra_columns = config['OBSERVED_COUNTS']['obs_extra_columns']

    # Convert column letters to numerical indices
    obs_usecols = config['OBSERVED_COUNTS']['obs_usecols']

    # Read the Obs data and the CHAMP estimation data
    obs_df = pd.read_csv(
        obs_filepath,
        usecols=obs_usecols)
    est_df = filter_and_aggregate(
        obs_filepath,
        loaded_network_directory,
        loaded_network_files,
        loaded_network_column_names,
        loaded_network_files_time,
        extra_columns,
        at_mapping_dict,
        ft_mapping_dict)

    # Part 1 - Scatter Plot Variables
    chosen_timeperiod = config['SCATTER_INPUT']['chosen_period']
    classification_col = config['SCATTER_INPUT']['classification_col']
    combined_df_cols = config['SCATTER_INPUT']['combined_df_cols']
    output_file_name = os.path.join(outdir, config['SCATTER_INPUT']['output_file_name'])

    fields1 = config['EST_SCATTER_PLOT']['fields']
    nominal_fields1 = config['EST_SCATTER_PLOT']['nominal_fields']
    x_field1 = config['EST_SCATTER_PLOT']['xfield']
    y_field1 = config['EST_SCATTER_PLOT']['yfield']
    name1 = config['EST_SCATTER_PLOT']['name']
    vega_est_output_path = os.path.join(outdir, config['EST_SCATTER_PLOT']['est_output_template'])

    fields2 = config['PERCENT_SCATTER_PLOT']['fields']
    nominal_fields2 = config['PERCENT_SCATTER_PLOT']['nominal_fields']
    x_field2 = config['PERCENT_SCATTER_PLOT']['xfield']
    y_field2 = config['PERCENT_SCATTER_PLOT']['yfield']
    name2 = config['PERCENT_SCATTER_PLOT']['name']
    vega_diffpercent_output_path = os.path.join(outdir, config['PERCENT_SCATTER_PLOT']['diffpercent_output_template'])
 


    # Part 2 - Validation Stats Variables
    combined_df_cols_stats = config['STATS_INPUT']['combined_df_cols']
    group_vars = ['Observed Volume', 'Loc Type', 'AT Group', 'FT Group']
    times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']
    output_file_name = os.path.join(outdir, config['STATS_INPUT']['output_file_name'])

    # Part 3 - Map Variables
    freeflow_path = config['MAP_INPUT']['freeflow_dir']
    shp_output_path = os.path.join(outdir, config['MAP_INPUT']['shp_out_dir'])
    output_name = os.path.join(outdir, config['MAP_INPUT']['output_filename'])

    # Part 1 - Scatter Plot
    scatter_plot(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                 fields1, nominal_fields1, x_field1, y_field1, name1, 
                 fields2, nominal_fields2, x_field2, y_field2, name2,
                 vega_est_output_path, vega_diffpercent_output_path)

    # Part 2 - Validation Stats
    time_period_dfs = prepare_time_period_dfs(
        est_df, obs_df, times, combined_df_cols_stats)
    generate_and_save_tables(outdir, time_period_dfs, group_vars)

    # Part 3 - Map
    merged_df = calculate_differences(est_df, obs_df, output_name)
    process_geospatial_data(merged_df, freeflow_path, shp_output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process TOML configuration file for validation.")
    parser.add_argument("config_path", type=str, help="Path to the TOML configuration file.")
    
    # Check if the script is running in an interactive environment or not
    if len(sys.argv) > 1 and "ipykernel_launcher" not in sys.argv[0]:
        # Running from command line, use the provided argument
        args = parser.parse_args()
    else:
        # Running in Jupyter or IPython, provide a default path
        default_config_path = r"X:\Projects\Miscellaneous\validation_simwrapper\roads2023\config.toml"  # Replace with your actual default config path
        print(f"Running in an interactive environment. Using default config path: {default_config_path}")
        args = argparse.Namespace(config_path=default_config_path)

    # Load the TOML configuration file
    config = toml.load(args.config_path)

    # Run the validation function with the loaded configuration
    validation_road(config)
