import toml
import yaml
import string
import pandas as pd
import argparse
from dataprocess import generate_loaded_network_file_names, filter_and_aggregate
from scatter import compute_and_save_errors, generate_vega_lite_json_est, generate_vega_lite_json_diffpercent, generate_yaml_config
from stats import prepare_time_period_dfs, generate_and_save_tables
from map import calculate_differences, process_geospatial_data, create_yaml_file


def csv_col_letter_to_num(letter):
    num = 0
    for char in letter:
        if char in string.ascii_letters:
            num = num * 26 + (ord(char.upper()) - ord('A')) + 1
    return num - 1


def scatter_plot_output(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                        fields1, nominal_fields1, x_field1, y_field1, name1, 
                        fields2, nominal_fields2, x_field2, y_field2, name2, 
                        yaml_file_path_scatter,
                        vega_est_output_path, vega_diffpercent_output_path):

    # Calculate the metrics
    select_time_period_loc_df, classification_col_types, file_name = compute_and_save_errors(
        est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name)

    # Generate the Scatter plots required files
    row_count = 1
    yaml_config = {'layout': {}}
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

        yaml_config['layout'][f'row{row_count}'] = generate_yaml_config(
            types, name1, name2)
        row_count += 1

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

    yaml_config['layout'][f'row{row_count}'] = generate_yaml_config(
        'all', name1, name2, include_all_data=True)
    
    with open(yaml_file_path_scatter, 'w') as file:
        yaml.dump(yaml_config, file, default_flow_style=False)


def scatter_plot(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                 fields1, nominal_fields1, x_field1, y_field1, name1, 
                 fields2, nominal_fields2, x_field2, y_field2, name2, 
                 yaml_file_path_scatter, vega_est_output_path, vega_diffpercent_output_path):
    # This function encapsulates the scatter plot generation logic
    scatter_plot_output(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                        fields1, nominal_fields1, x_field1, y_field1, name1, 
                        fields2, nominal_fields2, x_field2, y_field2, name2, 
                        yaml_file_path_scatter, vega_est_output_path, vega_diffpercent_output_path)
    return


def validation_road(config):
    # Extract the CHAMP input file names
    loaded_network_directory = config['LOADED_NETWORK']['loaded_network_directory']
    loaded_network_files_time = config['LOADED_NETWORK']['loaded_network_time']
    loaded_network_column_names = config['LOADED_NETWORK']['loaded_network_col_names']
    
    # Generate loaded_network file names
    loaded_network_files = generate_loaded_network_file_names(loaded_network_files_time)

    # Load mappings from the config file
    at_mapping = config['AT']['values']
    ft_mapping = config['FT']['values']

    # Extract the observed file name and tab
    obs_filepath = config['OBSERVED']['obs_filepath']
    extra_columns = config['OBSERVED']['obs_extra_columns']

    # Convert column letters to numerical indices
    obs_usecols = [csv_col_letter_to_num(col) for col in config['OBSERVED']['obs_usecols']]

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
        at_mapping,
        ft_mapping)

    # Part 1 - Scatter Plot Variables
    chosen_timeperiod = config['SCATTER_INPUT']['chosen_period']
    classification_col = config['SCATTER_INPUT']['classification_col']
    combined_df_cols = config['SCATTER_INPUT']['combined_df_cols']
    
    fields1 = config['EST_SCATTER_PLOT']['fields']
    nominal_fields1 = config['EST_SCATTER_PLOT']['nominal_fields']
    x_field1 = config['EST_SCATTER_PLOT']['xfield']
    y_field1 = config['EST_SCATTER_PLOT']['yfield']
    name1 = config['EST_SCATTER_PLOT']['name']
    vega_est_output_path = config['EST_SCATTER_PLOT']['est_output_template']

    fields2 = config['PERCENT_SCATTER_PLOT']['fields']
    nominal_fields2 = config['PERCENT_SCATTER_PLOT']['nominal_fields']
    x_field2 = config['PERCENT_SCATTER_PLOT']['xfield']
    y_field2 = config['PERCENT_SCATTER_PLOT']['yfield']
    name2 = config['PERCENT_SCATTER_PLOT']['name']
    vega_diffpercent_output_path = config['PERCENT_SCATTER_PLOT']['diffpercent_output_template']

    dashboard_num1 = config['SCATTER_YAML']['dashboard_number']
    yaml_file_path_scatter = config['SCATTER_YAML']['yaml_path'].format(dashboard_num1=dashboard_num1)

    # Part 2 - Validation Stats Variables
    combined_df_cols_stats = config['STATS_INPUT']['combined_df_cols']
    group_vars = ['Observed Volume', 'Loc Type', 'AT Group', 'FT Group']
    times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']
    output_file_name = config['STATS_INPUT']['output_file_name']

    # Part 3 - Map Variables
    freeflow_path = config['MAP_INPUT']['freeflow_dir']
    shp_output_path = config['MAP_INPUT']['shp_out_dir']
    output_name = config['MAP_INPUT']['output_filename']

    center = config['MAP_YAML']['center']
    data_file = config['MAP_YAML']['csv_file']
    shapes_file = config['MAP_YAML']['shape_file']
    join_field = config['MAP_YAML']['join']
    line_width_col = config['MAP_YAML']['line_wid_col']
    line_color_col = config['MAP_YAML']['line_color_col']
    breakpoints = config['MAP_YAML']['breakpoints']
    dashboard_num3 = config['MAP_YAML']['dashboard_number']
    yaml_file_path_map = config['MAP_YAML']['yaml_path'].format(dashboard_num3=dashboard_num3)

    # Part 1 - Scatter Plot
    scatter_plot(est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col, output_file_name,
                 fields1, nominal_fields1, x_field1, y_field1, name1, 
                 fields2, nominal_fields2, x_field2, y_field2, name2, 
                 yaml_file_path_scatter, vega_est_output_path, vega_diffpercent_output_path)

    # Part 2 - Validation Stats
    time_period_dfs = prepare_time_period_dfs(
        est_df, obs_df, times, combined_df_cols_stats)
    generate_and_save_tables(time_period_dfs, group_vars)

    # Part 3 - Map
    merged_df = calculate_differences(est_df, obs_df, output_name)
    process_geospatial_data(merged_df, freeflow_path, shp_output_path)
    create_yaml_file(
        center,
        shapes_file,
        data_file,
        join_field,
        line_width_col,
        line_color_col,
        breakpoints,
        yaml_file_path_map)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process TOML configuration file for validation.")
    parser.add_argument("config_path", type=str, help="Path to the TOML configuration file.")
    args = parser.parse_args()

    # Load the TOML configuration file
    config = toml.load(args.config_path)

    # Run the validation function with the loaded configuration
    validation_road(config)
