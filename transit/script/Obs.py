import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import os
import configparser

config = configparser.ConfigParser()
config.read('transit.ctl')
WORKING_FOLDER          =  config['folder_setting']['transit_output_dir']
OUTPUT_FOLDER           =  config['folder_setting']['markdown_output_dir']
INPUT_FOLDER            =  config['folder_setting']['transit_input_dir']
Transit_Templet         =  os.path.join(INPUT_FOLDER, config['transit']['Transit_Templet'])

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
    

def format_numeric(x):
    """ Format a numeric value with commas and no decimal places. """
    try:
        return f"{float(x):,.0f}" if x not in ['-', ''] else x
    except ValueError:
        return x
    
obs_MUNI_line = pd.read_excel(Transit_Templet, usecols = "B:H", sheet_name='obs_MUNI_line', skiprows = list(range(9)))
obs_MUNI_line['Ridership'] = obs_MUNI_line['Ridership'].apply(lambda x: format_numeric(x))
dataframe_to_markdown(obs_MUNI_line, os.path.join(OUTPUT_FOLDER,'obs_MUNI_line.md'), highlight_rows=None, center_align_columns=['Mode','Direction','TOD','Key_line_dir','Key_line_tod_dir'], column_widths = 100)

obs_BART_line = pd.read_excel(Transit_Templet, usecols = "B:F", sheet_name='obs_BART_station', skiprows = list(range(6)))
obs_BART_line['Boardings'] = obs_BART_line['Boardings'].apply(lambda x: format_numeric(x))
obs_BART_line['Alightings'] = obs_BART_line['Alightings'].apply(lambda x: format_numeric(x))
dataframe_to_markdown(obs_BART_line, os.path.join(OUTPUT_FOLDER,'obs_BART_station.md'), highlight_rows=None, center_align_columns=['TOD','Key'], column_widths = 100)

obs_BART_county = pd.read_excel(Transit_Templet, usecols = "B:F", sheet_name='obs_BART_county', skiprows = list(range(6)))
obs_BART_county['Boardings'] = obs_BART_county['Boardings'].apply(lambda x: format_numeric(x))
obs_BART_county['Alightings'] = obs_BART_county['Alightings'].apply(lambda x: format_numeric(x))
dataframe_to_markdown(obs_BART_county, os.path.join(OUTPUT_FOLDER,'obs_BART_county.md'), highlight_rows=None, center_align_columns=['TOD','Key'], column_widths = 100)

obs_BART_SL = pd.read_excel(Transit_Templet, usecols = "B:F", sheet_name='obs_BART_SL', skiprows = list(range(6))) 
obs_BART_SL['Ridership'] = obs_BART_SL['Ridership'].apply(lambda x: format_numeric(x))
dataframe_to_markdown(obs_BART_SL, os.path.join(OUTPUT_FOLDER,'obs_BART_SL.md'), highlight_rows=None, center_align_columns=['Direction','TOD','Key'], column_widths = 100)

obs_SL = pd.read_excel(Transit_Templet, usecols = "B:H", sheet_name='obs_Screenlines', skiprows = list(range(6))) 
obs_SL['Ridership'] = obs_SL['Ridership'].apply(lambda x: format_numeric(x))
dataframe_to_markdown(obs_SL, os.path.join(OUTPUT_FOLDER,'obs_Screenlines.md'), highlight_rows=None, center_align_columns=['Direction','TOD','Key', 'Operator', 'Mode'], column_widths = 100)


def NTD_to_df(Transit_Templet):
    df = pd.read_excel(Transit_Templet, usecols = "B:D", sheet_name='obs_NTD', skiprows = list(range(4)), header=None, engine='openpyxl')
    tables = []
    current_table_start = None

    for index, row in df.iterrows():
    # Check if the row contains 'Annual UPT' and '% of total ridership', which indicates a header
        if 'Annual UPT' in row.values and '% of total ridership' in row.values:
        # If we have a current table being built, add it to the tables list
            if current_table_start is not None:
                tables.append(df.iloc[current_table_start:index].reset_index(drop=True))
        # Set the start of the new table
            current_table_start = index
        
    if current_table_start is not None:
        tables.append(df.iloc[current_table_start:].reset_index(drop=True))

    for i, table in enumerate(tables):
        table.columns = table.iloc[0]
        table = table.drop(table.index[0])
        table = table.dropna(how='all', axis=0).reset_index(drop=True)
        table['Annual UPT'] = table['Annual UPT'].apply(lambda x: format_numeric(x))
        table["% of total ridership"] = table["% of total ridership"].apply(lambda x: f"{x:.2%}" if not pd.isna(x) else x)
        table = table.reset_index(drop=True)
        tables[i] = table
    return tables

tables = NTD_to_df(Transit_Templet)



def df_to_markdown_html(df, table_title):
    # Start the table and add the header
    html = f'<table>\n<thead>\n<tr>\n'
    html += f'<th style="text-align:left; width: 200px;"><strong>{table_title}</strong></th>'
    for col in df.columns[1:]:  # Assuming the first column is always present
        html += f'<th style="text-align:center; width: 150px;"><strong>{col}</strong></th>'
    html += '\n</tr>\n</thead>\n<tbody>\n'
    
    # Add the body of the table
    for index, row in df.iterrows():
        html += '<tr>\n'
        for idx, value in enumerate(row):
            # Check for NaN values and replace with an empty string
            if pd.isnull(value):
                value = ""
            else:
                # Apply bold formatting for the last 2 rows
                value = f'<strong>{value}</strong>' if index >= len(df) - 2 else value
            
            align = "left" if idx == 0 else "right"
            html += f'<td style="text-align:{align}">{value}</td>'
        html += '\n</tr>\n'
    
    # Close the table
    html += '</tbody>\n</table>\n'
    
    return html

# Assuming 'tables' is your list of DataFrames and each DataFrame has a proper header
with open(os.path.join(OUTPUT_FOLDER,'obs_NTD.md'), 'w') as md_file:
    for i, df in enumerate(tables):
        # You might need to adjust how you get 'table_title'
        table_title = df.columns[0] #if i == 0 else df.iloc[0,0]  # Example way to derive a title
        html_table = df_to_markdown_html(df, table_title)
        md_file.write(html_table + '\n\n')
