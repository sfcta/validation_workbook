import os
import pandas as pd
from transit_function import dataframe_to_markdown, format_numeric

# with open("transit.toml", "rb") as f:
#     config = tomllib.load(f)
# WORKING_FOLDER = config["directories"]["transit_output_dir"]
# OUTPUT_FOLDER = config["directories"]["markdown_output_dir"]
# INPUT_FOLDER = config["directories"]["transit_input_dir"]
# observed_BART = os.path.join(INPUT_FOLDER, config["transit"]["observed_BART"])
# observed_BART_county = os.path.join(
#     INPUT_FOLDER, config["transit"]["observed_BART_county"]
# )
# observed_BART_SL = os.path.join(INPUT_FOLDER, config["transit"]["observed_BART_SL"])
# observed_MUNI_Line = os.path.join(INPUT_FOLDER, config["transit"]["observed_MUNI_Line"])
# observed_SL = os.path.join(INPUT_FOLDER, config["transit"]["observed_SL"])
# observed_NTD = os.path.join(INPUT_FOLDER, config["transit"]["observed_NTD"])
def NTD_to_df(observed_NTD):
    df = pd.read_excel(
        observed_NTD,
        usecols="B:D",
        skiprows=list(range(4)),
        header=None,
        engine="openpyxl",
    )
    tables = []
    current_table_start = None

    for index, row in df.iterrows():
        # Check if the row contains 'Annual UPT' and '% of total ridership', which indicates a header
        if "Annual UPT" in row.values and "% of total ridership" in row.values:
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
        table = table.dropna(how="all", axis=0).reset_index(drop=True)
        table["Annual UPT"] = table["Annual UPT"].apply(lambda x: format_numeric(x))
        table["% of total ridership"] = table["% of total ridership"].apply(
            lambda x: f"{x:.2%}" if not pd.isna(x) else x
        )
        table = table.reset_index(drop=True)
        tables[i] = table
    return tables

def df_to_markdown_html(df, table_title):
    # Start the table and add the header
    html = "<table>\n<thead>\n<tr>\n"
    html += f'<th style="text-align:left; width: 200px;"><strong>{table_title}</strong></th>'
    for col in df.columns[1:]:  # Assuming the first column is always present
        html += (
            f'<th style="text-align:center; width: 150px;"><strong>{col}</strong></th>'
        )
    html += "\n</tr>\n</thead>\n<tbody>\n"

    # Add the body of the table
    for index, row in df.iterrows():
        html += "<tr>\n"
        for idx, value in enumerate(row):
            # Check for NaN values and replace with an empty string
            if pd.isnull(value):
                value = ""
            else:
                # Apply bold formatting for the last 2 rows
                value = f"<strong>{value}</strong>" if index >= len(df) - 2 else value

            align = "left" if idx == 0 else "right"
            html += f'<td style="text-align:{align}">{value}</td>'
        html += "\n</tr>\n"

    # Close the table
    html += "</tbody>\n</table>\n"

    return html

def process_obs_data(observed_MUNI_Line, OUTPUT_FOLDER, observed_BART, observed_BART_county, observed_BART_SL, observed_SL, observed_NTD):
        
    obs_MUNI_line = pd.read_csv(observed_MUNI_Line)
    obs_MUNI_line["Ridership"] = obs_MUNI_line["Ridership"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_MUNI_line,
        os.path.join(OUTPUT_FOLDER, "obs_MUNI_line.md"),
        highlight_rows=None,
        center_align_columns=[
            "Mode",
            "Direction",
            "TOD",
            "Key_line_dir",
            "Key_line_tod_dir",
        ],
        column_widths=100,
    )

    obs_BART_line = pd.read_csv(observed_BART)
    obs_BART_line["Boardings"] = obs_BART_line["Boardings"].apply(
        lambda x: format_numeric(x)
    )
    obs_BART_line["Alightings"] = obs_BART_line["Alightings"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_BART_line,
        os.path.join(OUTPUT_FOLDER, "obs_BART_station.md"),
        highlight_rows=None,
        center_align_columns=["TOD", "Key"],
        column_widths=100,
    )

    obs_BART_county = pd.read_csv(observed_BART_county)
    obs_BART_county["Boardings"] = obs_BART_county["Boardings"].apply(
        lambda x: format_numeric(x)
    )
    obs_BART_county["Alightings"] = obs_BART_county["Alightings"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_BART_county,
        os.path.join(OUTPUT_FOLDER, "obs_BART_county.md"),
        highlight_rows=None,
        center_align_columns=["TOD", "Key"],
        column_widths=100,
    )

    obs_BART_SL = pd.read_csv(observed_BART_SL)
    obs_BART_SL["Ridership"] = obs_BART_SL["Ridership"].apply(lambda x: format_numeric(x))
    dataframe_to_markdown(
        obs_BART_SL,
        os.path.join(OUTPUT_FOLDER, "obs_BART_SL.md"),
        highlight_rows=None,
        center_align_columns=["Direction", "TOD", "Key"],
        column_widths=100,
    )

    obs_SL = pd.read_csv(observed_SL)
    obs_SL["Ridership"] = obs_SL["Ridership"].apply(lambda x: format_numeric(x))
    dataframe_to_markdown(
        obs_SL,
        os.path.join(OUTPUT_FOLDER, "obs_Screenlines.md"),
        highlight_rows=None,
        center_align_columns=["Direction", "TOD", "Key", "Operator", "Mode"],
        column_widths=100,
    )

    tables = NTD_to_df(observed_NTD)

    # Assuming 'tables' is your list of DataFrames and each DataFrame has a proper header
    with open(os.path.join(OUTPUT_FOLDER, "obs_NTD.md"), "w") as md_file:
        for i, df in enumerate(tables):
            # You might need to adjust how you get 'table_title'
            table_title = df.columns[
                0
            ]  # if i == 0 else df.iloc[0,0]  # Example way to derive a title
            html_table = df_to_markdown_html(df, table_title)
            md_file.write(html_table + "\n\n")
