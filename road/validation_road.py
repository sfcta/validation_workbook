import toml
import yaml
import string
import pandas as pd
from dataprocess import *
from scatter import *
from stats import *
from map import *


def excel_col_letter_to_num(letter):
    num = 0
    for char in letter:
        if char in string.ascii_letters:
            num = num * 26 + (ord(char.upper()) - ord('A')) + 1
    return num - 1
    
def main():
    # Read the TOML configuration file
    config = toml.load('config.toml')

    # Extract the CHAMP input file names
    dbf_directory = config['DBF']['DBF_Directory']
    dbf_files_time = config['DBF']['DBF_Files_TIME']
    dbf_column_names = config['DBF']['DBF_Column_Names']
    
    # Generate DBF file names
    dbf_files = generate_dbf_file_names(dbf_files_time)

    # Load mappings from the ini file
    at_mapping = {int(key): value for key, value in config['AT'].items()}
    ft_mapping = {int(key): value for key, value in config['FT'].items()}

    # Extract the observed file name and tab
    excel_file_path = config['OBSERVED']['Excel_File_Path']
    sheet_name = config['OBSERVED']['Sheet_Name']
    extra_columns = config['OBSERVED']['Excel_Extra_Columns']
    
    # Convert column letters to numerical indices
    obs_usecols_letters = config['OBSERVED']['Obs_usecols']
    obs_usecols = [excel_col_letter_to_num(col) for col in obs_usecols_letters]
    
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
        extra_columns,
        at_mapping,
        ft_mapping)

    # Part 1
    # Scatter variables
    chosen_timeperiod = config['SCATTER_INPUT']['Chosen_period']
    classification_col = config['SCATTER_INPUT']['Classification_col']
    combined_df_cols = config['SCATTER_INPUT']['Combined_DF_Cols']

    # Est-Obs Plot
    fields1 = config['EST_SCATTER_PLOT']['Fields']
    nominal_fields1 = config['EST_SCATTER_PLOT']['NominalFields']
    x_field1 = config['EST_SCATTER_PLOT']['XField']
    y_field1 = config['EST_SCATTER_PLOT']['YField']
    name1 = config['EST_SCATTER_PLOT']['Name']

    # Percentile Plot
    fields2 = config['PERCENT_SCATTER_PLOT']['Fields']
    nominal_fields2 = config['PERCENT_SCATTER_PLOT']['NominalFields']
    x_field2 = config['PERCENT_SCATTER_PLOT']['XField']
    y_field2 = config['PERCENT_SCATTER_PLOT']['YField']
    name2 = config['PERCENT_SCATTER_PLOT']['Name']

    # Dashboard num
    dashboard_num1 = config['SCATTER_YAML']['Dashboard_number']

    # Part 2
    combined_df_cols_stats = config['STATS_INPUT']['Combined_DF_Cols']
    group_vars = ['Observed Volume', 'Loc Type', 'AT Group', 'FT Group']
    times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']

    # Part 3
    freeflow_path = config['MAP_INPUT']['Freeflow_Dir']
    shp_output_path = config['MAP_INPUT']['Shp_out_Dir']

    data_file = config['MAP_YAML']['Csv_file']
    shapes_file = config['MAP_YAML']['Shape_file']
    join_field = config['MAP_YAML']['Join']
    line_width_col = config['MAP_YAML']['Line_wid_col']
    line_color_col = config['MAP_YAML']['Line_color_col']
    breakpoints = config['MAP_YAML']['breakpoints']
    dashboard_num3 = config['MAP_YAML']['Dashboard_number']
    center = config['MAP_YAML']['Center']

    # Part 1 - Scatter Plot
    # Calculate the metrics
    Select_time_period_loc_df, classification_col_types, file_name = compute_and_save_errors(
        est_df, obs_df, chosen_timeperiod, combined_df_cols, classification_col)

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
            name1)
        # Generate percent errors vs obs scatter plot
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
    yaml_file_path = f"dashboard{dashboard_num1}-Scatter.yaml"
    with open(yaml_file_path, 'w') as file:
        yaml.dump(yaml_config, file, default_flow_style=False)

    # Part 2 - Validation Stats
    time_period_dfs = prepare_time_period_dfs(
        est_df, obs_df, times, combined_df_cols_stats)
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
        dashboard_num3)

if __name__ == "__main__":
    main()
