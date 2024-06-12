import shapefile
import pandas as pd
from os import listdir
from os.path import isfile, join
import os
import configparser

config = configparser.ConfigParser()
config.read('transit.ctl')
WORKING_FOLDER          =  config['folder_setting']['transit_output_dir']
OUTPUT_FOLDER           =  config['folder_setting']['markdown_output_dir']
INPUT_FOLDER            =  config['folder_setting']['transit_input_dir']
MUNI_output_dir         =  config['folder_setting']['MUNI_output_dir']
BART_output_dir         =  config['folder_setting']['BART_output_dir']
Screenline_output_dir   =  config['folder_setting']['Screenline_output_dir']
observed_BART           =  os.path.join(INPUT_FOLDER, config['transit']['observed_BART'])
observed_BART_county    =  os.path.join(INPUT_FOLDER, config['transit']['observed_BART_county'])
observed_BART_SL        =  os.path.join(INPUT_FOLDER, config['transit']['observed_BART_SL'])
observed_MUNI_Line      =  os.path.join(INPUT_FOLDER, config['transit']['observed_MUNI_Line'])
observed_SL             =  os.path.join(INPUT_FOLDER, config['transit']['observed_SL'])
model_BART              =  os.path.join(WORKING_FOLDER, config['output']['model_BART'])
model_BART_county       =  os.path.join(WORKING_FOLDER, config['output']['model_BART_county'])
model_BART_SL           =  os.path.join(WORKING_FOLDER, config['output']['model_BART_SL'])
model_MUNI_Line         =  os.path.join(WORKING_FOLDER, config['output']['model_MUNI_line'])
model_SL                =  os.path.join(WORKING_FOLDER, config['output']['model_SL'])

file_create = [ OUTPUT_FOLDER, MUNI_output_dir, BART_output_dir, Screenline_output_dir]
for path in file_create:
    if not os.path.exists(path):
        # If the folder does not exist, create it
        os.makedirs(path)
        print(f"Folder '{path}' did not exist and was created.")
    else:
        print(f"Folder '{path}' already exists.")

def dataframe_to_markdown(df, file_name='dataframe_table.md', highlight_rows=None, center_align_columns=None, column_widths = 100):
    """
    Convert a Pandas DataFrame to a custom Markdown table, highlight specific rows, right align specified columns,
    and save it to a file, with the first column always left-aligned in both header and data.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to convert.
    file_name (str): Name of the file to save the Markdown table.
    highlight_rows (list): List of row indices to highlight.
    right_align_columns (list): List of column names to right align.
    """
    if highlight_rows is None:
        highlight_rows = []
    if center_align_columns is None:
        center_align_columns = []

    # Start the Markdown table with the header
    md_output = "<table>\n<thead>\n<tr>\n"
    for i, col in enumerate(df.columns):
        # Left align for the first column header, center align for others
        header_align = "left" if i == 0 else "center"
        md_output += f"<th style=\"text-align:{header_align}; width: {column_widths}px;\"><strong>{col}</strong></th>\n"
    md_output += "</tr>\n</thead>\n<tbody>\n"

    # Add the table rows
    for index, row in df.iterrows():
        md_output += "<tr>\n"
        for i, col in enumerate(df.columns):
            cell_value = '' if pd.isna(row[col]) else row[col]

            # Determine the alignment based on the column name and index
            if i == 0:
                align = "left"  # Left align for the first column
            elif col in center_align_columns:
                align = "center"  # Right align for specified columns
            else:
                align = "right"  # Center align for other columns
            
            # Apply highlight if the row index is in the highlight_rows list
            if index in highlight_rows:
                md_output += f"<td style=\"text-align:{align}\"><strong>{cell_value}</strong></td>\n"
            else:
                md_output += f"<td style=\"text-align:{align}\">{cell_value}</td>\n"
        md_output += "</tr>\n"

    md_output += "</tbody>\n</table>"

    # Save to a Markdown file
    with open(file_name, 'w') as file:
        file.write(md_output)
    
    print(f"Markdown table saved to '{file_name}'")
    
def format_dataframe(df, numeric_columns, percentage_columns=None):
    """
    Format a DataFrame for readable display.
    - Fills NA values with '-'.
    - Formats specified numeric columns with commas and no decimal places.
    - Formats specified columns as percentages.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to format.
    numeric_columns (list): List of numeric column names to format.
    percentage_columns (list): List of column names to format as percentages.

    Returns:
    pd.DataFrame: The formatted DataFrame.
    """
    if percentage_columns is None:
        percentage_columns = []

    # Fill NA values
    formatted_df = df.fillna('-')

    # Format specified numeric columns
    for col in numeric_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_numeric(x))

    # Format percentage columns
    for col in percentage_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_percentage(x))

    return formatted_df

def format_numeric(x):
    """ Format a numeric value with commas and no decimal places. """
    try:
        return f"{float(x):,.0f}" if x not in ['-', ''] else x
    except ValueError:
        return x

def format_percentage(x):
    """ Format a value as a percentage. """
    try:
        return f"{float(x):.0f}%" if x not in ['-', ''] else x
    except ValueError:
        return x
    
def convert_to_integer(value):
    try:
        return int(value)
    except ValueError:
        return value
    
def process_data(obs_MUNI_line, model_MUNI_line, filters, groupby_column, sum_column, rename_column, join_method):
    """
    Process MUNI data with flexible filtering, grouping, and summing.

    Parameters:
    obs_MUNI_line (DataFrame): The observed MUNI line data.
    model_MUNI_line (DataFrame): The modeled MUNI line data.
    filters (list of tuples): List of filter conditions, where each tuple contains (column_name, value).
    groupby_column (str): Column name to group by.
    sum_column (str): Column name to sum.
    rename_column (str): New name to rename the groupby column to in the output.

    Returns:
    DataFrame: The processed and formatted DataFrame.
    """

    # Apply the custom function to convert values
    model_MUNI_line[groupby_column] = model_MUNI_line[groupby_column].apply(convert_to_integer)
    obs_MUNI_line[groupby_column] = obs_MUNI_line[groupby_column].apply(convert_to_integer)

    # Apply filters
    if filters is not None:
        for filter_col, filter_val in filters:
            obs_MUNI_line = obs_MUNI_line[obs_MUNI_line[filter_col] == filter_val]
            model_MUNI_line = model_MUNI_line[model_MUNI_line[filter_col] == filter_val]

    # Processing observed data
    MUNI_IB_obs = obs_MUNI_line.groupby(groupby_column)[sum_column].sum().reset_index()
    MUNI_IB_obs.rename(columns={groupby_column: rename_column, sum_column: 'Observed'}, inplace=True)

    # Processing modeled data
    MUNI_IB_model = model_MUNI_line.groupby(groupby_column)[sum_column].sum().reset_index()
    MUNI_IB_model.rename(columns={groupby_column: rename_column, sum_column: 'Modeled'}, inplace=True)

    # Merging observed and modeled data
    MUNI_IB = pd.merge(MUNI_IB_obs, MUNI_IB_model, on=rename_column, how = join_method)

    # Calculating total row and appending it
    total_row = pd.Series(MUNI_IB[['Observed', 'Modeled']].sum(), name='Total')
    MUNI_IB = pd.concat([MUNI_IB, total_row.to_frame().T], ignore_index=True)

    # Calculating 'Diff' and 'Percentage Diff'
    MUNI_IB['Diff'] = (MUNI_IB['Modeled'] - MUNI_IB['Observed'])
    MUNI_IB['Percentage Diff'] = (MUNI_IB['Diff'] * 100 / MUNI_IB['Observed'])

    # Assign 'Total' label to the last row in the renamed column
    MUNI_IB.at[MUNI_IB.index[-1], rename_column] = 'Total'

    # Formatting the DataFrame
    numeric_cols = ['Observed', 'Modeled', 'Diff']
    MUNI_IB = format_dataframe(MUNI_IB, numeric_columns=numeric_cols, percentage_columns=['Percentage Diff'])

    return MUNI_IB


obs_MUNI_line = pd.read_csv(observed_MUNI_Line)  
model_MUNI_line = pd.read_csv(model_MUNI_Line)
tod_order = ['EA', 'AM', 'MD', 'PM', 'EV', 'Total']
MUNI_IB = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'IB')], 'Line', 'Ridership', 'Route', 'left')
dataframe_to_markdown(MUNI_IB, file_name=os.path.join(OUTPUT_FOLDER, 'MUNI_ib_day.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_OB = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'OB')], 'Line', 'Ridership', 'Route', 'left')
MUNI_OB = pd.merge(MUNI_IB[['Route']],MUNI_OB, on='Route', how='left')
dataframe_to_markdown(MUNI_OB, file_name=os.path.join(OUTPUT_FOLDER, 'MUNI_ob_day.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_IB.to_csv(os.path.join(MUNI_output_dir,"MUNI_IB.csv"), index=False)
MUNI_OB.to_csv(os.path.join(MUNI_output_dir,"MUNI_OB.csv"), index=False)
MUNI_IB_AM = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'IB'), ('TOD', 'AM')], 'Line', 'Ridership', 'Route', 'left')
MUNI_IB_AM = pd.merge(MUNI_IB[['Route']],MUNI_IB_AM, on='Route', how='left')
MUNI_IB_AM = MUNI_IB_AM.fillna('-')
dataframe_to_markdown(MUNI_IB_AM, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_ib_am.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_IB_PM = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'IB'), ('TOD', 'PM')], 'Line', 'Ridership', 'Route', 'left')
MUNI_IB_PM = pd.merge(MUNI_IB[['Route']],MUNI_IB_PM, on='Route', how='left')
MUNI_IB_PM = MUNI_IB_PM.fillna('-')
dataframe_to_markdown(MUNI_IB_PM, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_ib_pm.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_OB_AM = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'OB'), ('TOD', 'AM')], 'Line', 'Ridership', 'Route', 'left')
MUNI_OB_AM = pd.merge(MUNI_IB[['Route']],MUNI_OB_AM, on='Route', how='left')
MUNI_OB_AM = MUNI_OB_AM.fillna('-')
dataframe_to_markdown(MUNI_OB_AM, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_ob_am.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_OB_PM = process_data(obs_MUNI_line, model_MUNI_line, [('Direction', 'OB'), ('TOD', 'PM')], 'Line', 'Ridership', 'Route', 'left')
MUNI_OB_PM = pd.merge(MUNI_IB[['Route']],MUNI_OB_PM, on='Route', how='left')
MUNI_OB_PM = MUNI_OB_PM.fillna('-')
dataframe_to_markdown(MUNI_OB_PM, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_ob_pm.md'), highlight_rows=[72], center_align_columns=None, column_widths = 70)
MUNI_mode = process_data(obs_MUNI_line, model_MUNI_line, None, 'Mode', 'Ridership', 'Mode', 'left')
dataframe_to_markdown(MUNI_mode, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_mode_day.md'), highlight_rows=[3], center_align_columns=None, column_widths = 70)
MUNI_mode[~MUNI_mode['Mode'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,'MUNI_mode.csv'), index=False)
MUNI_mode_am = process_data(obs_MUNI_line, model_MUNI_line, [('TOD', 'AM')], 'Mode', 'Ridership', 'Mode', 'left')
dataframe_to_markdown(MUNI_mode_am, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_mode_am.md'), highlight_rows=[3], center_align_columns=None, column_widths = 70)
MUNI_mode_am[~MUNI_mode_am['Mode'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,'MUNI_mode_am.csv'), index=False)
MUNI_mode_pm = process_data(obs_MUNI_line, model_MUNI_line,[('TOD', 'PM')], 'Mode', 'Ridership', 'Mode', 'left')
dataframe_to_markdown(MUNI_mode_pm, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_mode_pm.md'), highlight_rows=[3], center_align_columns=None, column_widths = 70)
MUNI_mode_pm[~MUNI_mode_pm['Mode'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,'MUNI_mode_pm.csv'), index=False)
MUNI_tod = process_data(obs_MUNI_line, model_MUNI_line, None, 'TOD', 'Ridership', 'TOD', 'left')
MUNI_tod['TOD'] = pd.Categorical(MUNI_tod['TOD'], categories=tod_order, ordered=True)
MUNI_tod = MUNI_tod.sort_values('TOD')
dataframe_to_markdown(MUNI_tod, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_tod.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
MUNI_tod[~MUNI_tod['TOD'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,'MUNI_tod.csv'), index=False)
MUNI_EB = process_data(obs_MUNI_line, model_MUNI_line, [('Mode', 'Express Bus')], 'TOD','Ridership', 'TOD', 'left')
MUNI_EB['TOD'] = pd.Categorical(MUNI_EB['TOD'], categories=tod_order, ordered=True)
MUNI_EB = MUNI_EB.sort_values('TOD')
dataframe_to_markdown(MUNI_EB, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_EB.md'), highlight_rows= [4], center_align_columns=None, column_widths = 70)
MUNI_EB[~MUNI_EB['TOD'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,"MUNI_EB.csv"), index=False)
MUNI_LB = process_data(obs_MUNI_line, model_MUNI_line, [('Mode', 'Local Bus')], 'TOD','Ridership', 'TOD', 'left')
MUNI_LB['TOD'] = pd.Categorical(MUNI_LB['TOD'], categories=tod_order, ordered=True)
MUNI_LB = MUNI_LB.sort_values('TOD')
dataframe_to_markdown(MUNI_LB, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_LB.md'), highlight_rows= [5], center_align_columns=None, column_widths = 70)
MUNI_LB[~MUNI_LB['TOD'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,"MUNI_LB.csv"), index=False)
MUNI_Rail = process_data(obs_MUNI_line, model_MUNI_line, [('Mode', 'Rail')], 'TOD','Ridership', 'TOD', 'left')
MUNI_Rail['TOD'] = pd.Categorical(MUNI_Rail['TOD'], categories=tod_order, ordered=True)
MUNI_Rail = MUNI_Rail.sort_values('TOD')
dataframe_to_markdown(MUNI_Rail, file_name=os.path.join(OUTPUT_FOLDER,'MUNI_Rail.md'), highlight_rows= [5], center_align_columns=None, column_widths = 70)
MUNI_Rail[~MUNI_Rail['TOD'].isin(['Total'])].to_csv(os.path.join(MUNI_output_dir,"MUNI_Rail.csv"), index=False)


#Valdiation for BART
def append_group_total(df, stations, group_name):
    # Filter the DataFrame for the specified stations
    df_group = df[df['Station'].isin(stations)]
    
    # Calculate the sum for the filtered rows
    group_total = pd.Series(df_group[['Observed', 'Modeled']].sum(), name=group_name)
    
    # Append the total row to the DataFrame with the group name
    df = pd.concat([df, group_total.to_frame().T], ignore_index=True)
    df.at[df.index[-1], 'Station'] = group_name
    return df

def process_BART_data(obs_MUNI_line, model_MUNI_line, direction_filter, tod_filter, groupby_column, sum_column):
    """
    Process MUNI data from Excel and CSV files with flexible grouping and summing.
    
    Parameters:
    excel_file_path (str): Path to the Excel file.
    csv_file_path (str): Path to the CSV file.
    direction_filter (str): Direction to filter the data by.
    groupby_column (str): Column name to group by.
    sum_column (str): Column name to sum.
    convert_to_integer_function (function): Function to convert column values to integers.
    format_dataframe_function (function): Function to format the DataFrame.

    Returns:
    pd.DataFrame: The processed and formatted DataFrame.
    """

    # Apply the custom function to convert values
    model_MUNI_line[groupby_column] = model_MUNI_line[groupby_column].apply(convert_to_integer)
    
    # Determine filter conditions
    obs_condition = pd.Series([True] * len(obs_MUNI_line))
    model_condition = pd.Series([True] * len(model_MUNI_line))

    if direction_filter is not None:
        obs_condition &= (obs_MUNI_line['Direction'] == direction_filter)
        model_condition &= (model_MUNI_line['Direction'] == direction_filter)

    if tod_filter is not None:
        obs_condition &= (obs_MUNI_line['TOD'] == tod_filter)
        model_condition &= (model_MUNI_line['TOD'] == tod_filter)

    # Processing observed data
    MUNI_IB_obs = obs_MUNI_line[obs_condition].groupby(groupby_column)[sum_column].sum().reset_index()
    MUNI_IB_obs.rename(columns={groupby_column: 'Station', sum_column: 'Observed'}, inplace=True)

    # Processing modeled data
    MUNI_IB_model = model_MUNI_line[model_condition].groupby(groupby_column)[sum_column].sum().reset_index()
    MUNI_IB_model.rename(columns={groupby_column: 'Station', sum_column: 'Modeled'}, inplace=True)

    # Merging observed and modeled data
    MUNI_IB = pd.merge(MUNI_IB_obs, MUNI_IB_model, on='Station', how='left')
    all = ['EMBR', 'MONT', 'POWL', 'CIVC', '16TH', '24TH', 'GLEN', 'BALB', '12TH', '19TH', 'DALY']
    MUNI_IB = MUNI_IB[MUNI_IB['Station'].isin(all)]

    # Calculating total row and appending it
    # Define your station groups
    station_core_SF = ['EMBR', 'MONT', 'POWL', 'CIVC']
    station_outer_SF = ['16TH', '24TH', 'GLEN', 'BALB']
    station_oakland = ['12TH', '19TH']

    # Use the function to append totals for each group
    MUNI_IB = append_group_total(MUNI_IB, station_core_SF, 'Core SF')
    MUNI_IB = append_group_total(MUNI_IB, station_outer_SF, 'Outer SF')
    MUNI_IB = append_group_total(MUNI_IB, station_oakland, 'Oakland Core')
    MUNI_IB = append_group_total(MUNI_IB, all, 'All Listed Stations')
    
    # Calculating 'Diff' and 'Percentage Diff'
    MUNI_IB['Diff'] = (MUNI_IB['Modeled'] - MUNI_IB['Observed'])
    MUNI_IB['Percentage Diff'] = (MUNI_IB['Diff'] * 100 / MUNI_IB['Observed'])

    # Formatting the DataFrame
    numeric_cols = ['Observed', 'Modeled', 'Diff']
    MUNI_IB = format_dataframe(MUNI_IB, numeric_columns=numeric_cols, percentage_columns=['Percentage Diff'])

    return MUNI_IB

def sort_dataframe_by_custom_order(df, order_column, custom_order):
    """
    Sort the DataFrame rows according to a custom order specified for one column.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to sort.
    order_column (str): The name of the column to apply the custom order.
    custom_order (list): The list specifying the custom order of values.
    
    Returns:
    pd.DataFrame: The sorted DataFrame.
    """
    # Create a categorical data type with the custom order
    category_order = pd.CategoricalDtype(categories=custom_order, ordered=True)
    
    # Convert the order column to this categorical type
    df[order_column] = pd.Categorical(df[order_column], categories=custom_order, ordered=True)
    
    # Sort the DataFrame by the order column
    df = df.sort_values(by=order_column)
    
    # Reset index after sorting
    df = df.reset_index(drop=True)
    
    return df

# Custom order based on the provided list
custom_order = [
    '19TH', '12TH', 'EMBR', 'MONT', 'POWL', 'CIVC',
    '16TH', '24TH', 'GLEN', 'BALB', 'DALY', 'Core SF',
    'Outer SF', 'Oakland Core', 'All Listed Stations'
]


# BART
obs_BART_line = pd.read_csv(observed_BART)  
model_BART_line = pd.read_csv(model_BART)
BART_boarding_allday = process_BART_data(obs_BART_line, model_BART_line, None, None, 'Station', 'Boardings')
BART_boarding_allday= sort_dataframe_by_custom_order(BART_boarding_allday, 'Station', custom_order)
dataframe_to_markdown(BART_boarding_allday, file_name=os.path.join(OUTPUT_FOLDER,'BART_boarding_allday.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)
BART_boarding_allday.to_csv(os.path.join(BART_output_dir, 'BART_boarding_allday.csv'), index=False)
BART_boarding_am = process_BART_data(obs_BART_line, model_BART_line, None, 'AM', 'Station', 'Boardings')
BART_boarding_am= sort_dataframe_by_custom_order(BART_boarding_am, 'Station', custom_order)
dataframe_to_markdown(BART_boarding_am, file_name=os.path.join(OUTPUT_FOLDER,'BART_boarding_am.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)
BART_boarding_pm = process_BART_data(obs_BART_line, model_BART_line, None, 'PM', 'Station', 'Boardings')
BART_boarding_pm= sort_dataframe_by_custom_order(BART_boarding_pm, 'Station', custom_order)
dataframe_to_markdown(BART_boarding_pm, file_name=os.path.join(OUTPUT_FOLDER,'BART_boarding_pm.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)
BART_at_allday = process_BART_data(obs_BART_line, model_BART_line, None, None, 'Station', 'Alightings')
BART_at_allday= sort_dataframe_by_custom_order(BART_at_allday, 'Station', custom_order)
dataframe_to_markdown(BART_at_allday, file_name=os.path.join(OUTPUT_FOLDER,'BART_at_allday.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)
BART_at_allday.to_csv(os.path.join(BART_output_dir,"BART_at_allday.csv"), index=False)
BART_at_am = process_BART_data(obs_BART_line, model_BART_line, None, 'AM', 'Station', 'Alightings')
BART_at_am= sort_dataframe_by_custom_order(BART_at_am, 'Station', custom_order)
dataframe_to_markdown(BART_at_am, file_name=os.path.join(OUTPUT_FOLDER,'BART_at_am.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)
BART_at_pm = process_BART_data(obs_BART_line, model_BART_line, None, 'PM', 'Station', 'Alightings')
BART_at_pm= sort_dataframe_by_custom_order(BART_at_pm, 'Station', custom_order)
dataframe_to_markdown(BART_at_pm, file_name=os.path.join(OUTPUT_FOLDER,'BART_at_pm.md'), highlight_rows=[11, 12, 13, 14], center_align_columns=None, column_widths = 80)


obs_BART_county = pd.read_csv(observed_BART_county)  
model_BART_county = pd.read_csv(model_BART_county)
county_order = ['San Francisco', 'San Mateo', 'Contra Costa', 'Alameda', 'Total']
county_br_day = process_data(obs_BART_county, model_BART_county, None, 'County', 'Boardings', 'County', 'left')
county_br_day['County'] = pd.Categorical(county_br_day['County'], categories=county_order, ordered=True)
county_br_day = county_br_day.sort_values('County')
dataframe_to_markdown(county_br_day, file_name=os.path.join(OUTPUT_FOLDER,'county_br_day.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_br_am = process_data(obs_BART_county, model_BART_county,[('TOD', 'AM')], 'County', 'Boardings', 'County', 'left')
county_br_am['County'] = pd.Categorical(county_br_am['County'], categories=county_order, ordered=True)
county_br_am = county_br_am.sort_values('County')
dataframe_to_markdown(county_br_am, file_name=os.path.join(OUTPUT_FOLDER,'county_br_am.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_br_pm = process_data(obs_BART_county, model_BART_county, [('TOD', 'PM')], 'County', 'Boardings', 'County', 'left')
county_br_pm['County'] = pd.Categorical(county_br_pm['County'], categories=county_order, ordered=True)
county_br_pm = county_br_pm.sort_values('County')
dataframe_to_markdown(county_br_pm, file_name=os.path.join(OUTPUT_FOLDER,'county_br_pm.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_br_day.to_csv(os.path.join(BART_output_dir,"county_br_day.csv"), index=False)
county_br_am.to_csv(os.path.join(BART_output_dir,"county_br_am.csv"), index=False)
county_br_pm.to_csv(os.path.join(BART_output_dir,"county_br_pm.csv"), index=False)
county_at_day = process_data(obs_BART_county, model_BART_county, None, 'County', 'Alightings', 'County', 'left')
county_at_day['County'] = pd.Categorical(county_at_day['County'], categories=county_order, ordered=True)
county_at_day = county_at_day.sort_values('County')
dataframe_to_markdown(county_at_day, file_name=os.path.join(OUTPUT_FOLDER,'county_at_day.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_at_am = process_data(obs_BART_county, model_BART_county, [('TOD', 'AM')], 'County', 'Alightings', 'County', 'left')
county_at_am['County'] = pd.Categorical(county_at_am['County'], categories=county_order, ordered=True)
county_at_am = county_at_am.sort_values('County')
dataframe_to_markdown(county_at_am, file_name=os.path.join(OUTPUT_FOLDER,'county_at_am.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_at_pm = process_data(obs_BART_county, model_BART_county, [('TOD', 'PM')], 'County', 'Alightings', 'County', 'left')
county_at_pm['County'] = pd.Categorical(county_at_pm['County'], categories=county_order, ordered=True)
county_at_pm = county_at_pm.sort_values('County')
dataframe_to_markdown(county_at_pm, file_name=os.path.join(OUTPUT_FOLDER,'county_at_pm.md'), highlight_rows=[4], center_align_columns=None, column_widths = 90)
county_at_day.to_csv(os.path.join(BART_output_dir,"county_at_day.csv"), index=False)
county_at_am.to_csv(os.path.join(BART_output_dir,"county_at_am.csv"), index=False)
county_at_pm.to_csv(os.path.join(BART_output_dir,"county_at_pm.csv"), index=False)


# BART Screenline
obs_BART_SL = pd.read_csv(observed_BART_SL)  
model_BART_SL = pd.read_csv(model_BART_SL)
transbay_BART_IB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Transbay'),('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
transbay_BART_IB['TOD'] = pd.Categorical(transbay_BART_IB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
transbay_BART_IB = transbay_BART_IB.sort_values('TOD')
dataframe_to_markdown(transbay_BART_IB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_BART_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_BART_OB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Transbay'),('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
transbay_BART_OB['TOD'] = pd.Categorical(transbay_BART_OB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
transbay_BART_OB = transbay_BART_OB.sort_values('TOD')
dataframe_to_markdown(transbay_BART_OB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_BART_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_BART_OB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Countyline'),('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
Countyline_BART_OB['TOD'] = pd.Categorical(Countyline_BART_OB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
Countyline_BART_OB = Countyline_BART_OB.sort_values('TOD')
dataframe_to_markdown(Countyline_BART_OB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_BART_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_BART_IB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Countyline'),('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
Countyline_BART_IB['TOD'] = pd.Categorical(Countyline_BART_IB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
Countyline_BART_IB = Countyline_BART_IB.sort_values('TOD')
dataframe_to_markdown(Countyline_BART_IB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_BART_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Intra_SF_BART_OB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Intra-SF'),('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
Intra_SF_BART_OB['TOD'] = pd.Categorical(Intra_SF_BART_OB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
Intra_SF_BART_OB = Intra_SF_BART_OB.sort_values('TOD')
dataframe_to_markdown(Intra_SF_BART_OB, file_name=os.path.join(OUTPUT_FOLDER,'SF_out.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Intra_SF_BART_IB = process_data(obs_BART_SL, model_BART_SL, [('Screenline', 'Intra-SF'),('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
Intra_SF_BART_IB['TOD'] = pd.Categorical(Intra_SF_BART_IB['TOD'], categories=tod_order, ordered=True)
# Sort the DataFrame by the 'TOD' column
Intra_SF_BART_IB = Intra_SF_BART_IB.sort_values('TOD')
dataframe_to_markdown(Intra_SF_BART_IB, file_name=os.path.join(OUTPUT_FOLDER,'SF_in.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_BART_IB[~transbay_BART_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir, "transbay_BART_IB.csv"), index=False)
transbay_BART_OB[~transbay_BART_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir, "transbay_BART_OB.csv"), index=False)
Countyline_BART_IB[~Countyline_BART_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir, "Countyline_BART_IB.csv"), index=False)
Countyline_BART_OB[~Countyline_BART_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir, "Countyline_BART_OB.csv"), index=False)
Intra_SF_BART_IB[~Intra_SF_BART_IB['TOD'].isin(['Total'])].to_csv(os.path.join(BART_output_dir, "Intra_SF_BART_IB.csv"), index=False)
Intra_SF_BART_OB[~Intra_SF_BART_OB['TOD'].isin(['Total'])].to_csv(os.path.join(BART_output_dir, "Intra_SF_BART_OB.csv"), index=False)


#Valdiation for Screenlines
obs_SL = pd.read_csv(observed_SL)
obs_SL['Ridership'] = obs_SL['Ridership'].replace({'-': '0', ' -   ': '0'}).str.replace(',', '').astype(float)
model_SL = pd.read_csv(model_SL)
transbay_AC_IB = process_data(obs_SL, model_SL, [('Screenline', 'Transbay'),('Operator', 'AC Transit'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
transbay_AC_IB['TOD'] = pd.Categorical(transbay_AC_IB['TOD'], categories=tod_order, ordered=True)
transbay_AC_IB = transbay_AC_IB.sort_values('TOD')
dataframe_to_markdown(transbay_AC_IB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_AC_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_AC_OB = process_data(obs_SL, model_SL, [('Screenline', 'Transbay'),('Operator', 'AC Transit'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD','right')
transbay_AC_OB['TOD'] = pd.Categorical(transbay_AC_OB['TOD'], categories=tod_order, ordered=True)
transbay_AC_OB = transbay_AC_OB.sort_values('TOD')
dataframe_to_markdown(transbay_AC_OB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_AC_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_overall_IB = process_data(obs_SL, model_SL, [('Screenline', 'Transbay'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
transbay_overall_IB['TOD'] = pd.Categorical(transbay_overall_IB['TOD'], categories=tod_order, ordered=True)
transbay_overall_IB = transbay_overall_IB.sort_values('TOD')
dataframe_to_markdown(transbay_overall_IB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_overall_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_overall_OB = process_data(obs_SL, model_SL, [('Screenline', 'Transbay'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
transbay_overall_OB['TOD'] = pd.Categorical(transbay_overall_OB['TOD'], categories=tod_order, ordered=True)
transbay_overall_OB = transbay_overall_OB.sort_values('TOD')
dataframe_to_markdown(transbay_overall_OB, file_name=os.path.join(OUTPUT_FOLDER,'transbay_overall_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
transbay_overall_IB[~transbay_overall_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"transbay_overall_IB.csv"), index=False)
transbay_overall_OB[~transbay_overall_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"transbay_overall_OB.csv"), index=False)
transbay_AC_IB[~transbay_AC_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"transbay_AC_IB.csv"), index=False)
transbay_AC_OB[~transbay_AC_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"transbay_AC_OB.csv"), index=False)
Countyline_CalTrain_IB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'),('Operator', 'CalTrain'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'right')
Countyline_CalTrain_IB['TOD'] = pd.Categorical(Countyline_CalTrain_IB['TOD'], categories=tod_order, ordered=True)
Countyline_CalTrain_IB = Countyline_CalTrain_IB.sort_values('TOD')
dataframe_to_markdown(Countyline_CalTrain_IB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_CalTrain_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_CalTrain_OB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'),('Operator', 'CalTrain'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD','right')
Countyline_CalTrain_OB['TOD'] = pd.Categorical(Countyline_CalTrain_OB['TOD'], categories=tod_order, ordered=True)
Countyline_CalTrain_OB = Countyline_CalTrain_OB.sort_values('TOD')
dataframe_to_markdown(Countyline_CalTrain_OB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_CalTrain_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_SamTrans_IB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'),('Operator', 'SamTrans'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
Countyline_SamTrans_IB['TOD'] = pd.Categorical(Countyline_SamTrans_IB['TOD'], categories=tod_order, ordered=True)
Countyline_SamTrans_IB = Countyline_SamTrans_IB.sort_values('TOD')
dataframe_to_markdown(Countyline_SamTrans_IB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_SamTrans_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_SamTrans_OB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'),('Operator', 'SamTrans'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD','left')
Countyline_SamTrans_OB['TOD'] = pd.Categorical(Countyline_SamTrans_OB['TOD'], categories=tod_order, ordered=True)
Countyline_SamTrans_OB = Countyline_SamTrans_OB.sort_values('TOD')
dataframe_to_markdown(Countyline_SamTrans_OB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_SamTrans_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_overall_IB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
Countyline_overall_IB['TOD'] = pd.Categorical(Countyline_overall_IB['TOD'], categories=tod_order, ordered=True)
Countyline_overall_IB = Countyline_overall_IB.sort_values('TOD')
dataframe_to_markdown(Countyline_overall_IB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_overall_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_overall_OB = process_data(obs_SL, model_SL, [('Screenline', 'Countyline'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
Countyline_overall_OB['TOD'] = pd.Categorical(Countyline_overall_OB['TOD'], categories=tod_order, ordered=True)
Countyline_overall_OB = Countyline_overall_OB.sort_values('TOD')
dataframe_to_markdown(Countyline_overall_OB, file_name=os.path.join(OUTPUT_FOLDER,'Countyline_overall_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
Countyline_CalTrain_IB[~Countyline_CalTrain_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_CalTrain_IB.csv"), index=False)
Countyline_CalTrain_OB[~Countyline_CalTrain_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_CalTrain_OB.csv"), index=False)
Countyline_SamTrans_IB[~Countyline_SamTrans_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_SamTrans_IB.csv"), index=False)
Countyline_SamTrans_OB[~Countyline_SamTrans_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_SamTrans_OB.csv"), index=False)
Countyline_overall_IB[~Countyline_overall_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_overall_IB.csv"), index=False)
Countyline_overall_OB[~Countyline_overall_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"Countyline_overall_OB.csv"), index=False)
GG_Transit_IB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'),('Operator', 'Golden Gate Transit'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
GG_Transit_IB['TOD'] = pd.Categorical(GG_Transit_IB['TOD'], categories=tod_order, ordered=True)
GG_Transit_IB = GG_Transit_IB.sort_values('TOD')
dataframe_to_markdown(GG_Transit_IB, file_name=os.path.join(OUTPUT_FOLDER,'GG_Transit_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_Transit_OB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'),('Operator', 'Golden Gate Transit'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD','right')
GG_Transit_OB['TOD'] = pd.Categorical(GG_Transit_OB['TOD'], categories=tod_order, ordered=True)
GG_Transit_OB = GG_Transit_OB.sort_values('TOD')
dataframe_to_markdown(GG_Transit_OB, file_name=os.path.join(OUTPUT_FOLDER,'GG_Transit_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_Ferry_IB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'),('Operator', 'Golden Gate Ferry'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
GG_Ferry_IB['TOD'] = pd.Categorical(GG_Ferry_IB['TOD'], categories=tod_order, ordered=True)
GG_Ferry_IB = GG_Ferry_IB.sort_values('TOD')
dataframe_to_markdown(GG_Ferry_IB, file_name=os.path.join(OUTPUT_FOLDER,'GG_Ferry_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_Ferry_OB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'),('Operator', 'Golden Gate Ferry'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD','right')
GG_Ferry_OB['TOD'] = pd.Categorical(GG_Ferry_OB['TOD'], categories=tod_order, ordered=True)
GG_Ferry_OB = GG_Ferry_OB.sort_values('TOD')
dataframe_to_markdown(GG_Ferry_OB, file_name=os.path.join(OUTPUT_FOLDER,'GG_Ferry_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_overall_IB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'), ('Direction', 'IB')], 'TOD', 'Ridership', 'TOD', 'left')
GG_overall_IB['TOD'] = pd.Categorical(GG_overall_IB['TOD'], categories=tod_order, ordered=True)
GG_overall_IB = GG_overall_IB.sort_values('TOD')
dataframe_to_markdown(GG_overall_IB, file_name=os.path.join(OUTPUT_FOLDER,'GG_overall_IB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_overall_OB = process_data(obs_SL, model_SL, [('Screenline', 'Golden Gate'), ('Direction', 'OB')], 'TOD', 'Ridership', 'TOD', 'left')
GG_overall_OB['TOD'] = pd.Categorical(GG_overall_OB['TOD'], categories=tod_order, ordered=True)
GG_overall_OB = GG_overall_OB.sort_values('TOD')
dataframe_to_markdown(GG_overall_OB, file_name=os.path.join(OUTPUT_FOLDER,'GG_overall_OB.md'), highlight_rows=[5], center_align_columns=None, column_widths = 70)
GG_Transit_IB[~GG_Transit_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_Transit_IB.csv"), index=False)
GG_Transit_OB[~GG_Transit_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_Transit_OB.csv"), index=False)
GG_Ferry_IB[~GG_Ferry_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_Ferry_IB.csv"), index=False)
GG_Ferry_OB[~GG_Ferry_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_Ferry_OB.csv"), index=False)
GG_overall_IB[~GG_overall_IB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_overall_IB.csv"), index=False)
GG_overall_OB[~GG_overall_OB['TOD'].isin(['Total'])].to_csv(os.path.join(Screenline_output_dir,"GG_overall_OB.csv"), index=False)