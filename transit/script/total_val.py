import os
import tomllib

import geopandas as gpd
import pandas as pd

# we should be importing functions in this file into transit.py instead
# HOTFIX TODO pass results of read_transit_assignments() directly as arg
from transit import read_dbf_and_groupby_sum, transit_assignment_filepaths

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)


model_run_dir = config["directories"]["model_run"]
transit_assignments = transit_assignment_filepaths(model_run_dir)

WORKING_FOLDER = config["directories"]["transit_output_dir"]
OUTPUT_FOLDER = config["directories"]["markdown_output_dir"]
INPUT_FOLDER = config["directories"]["transit_input_dir"]
total_output_dir = config["directories"]["total_output_dir"]
Transit_Templet = os.path.join(INPUT_FOLDER, config["transit"]["Transit_Templet"])
observed_NTD = os.path.join(INPUT_FOLDER, config["transit"]["observed_NTD"])

# Create output file if is not exsits
file_create = [OUTPUT_FOLDER, total_output_dir]
for path in file_create:
    if not os.path.exists(path):
        # If the folder does not exist, create it
        os.makedirs(path)
        print(f"Folder '{path}' did not exist and was created.")
    else:
        print(f"Folder '{path}' already exists.")


# function to create markdown file
def dataframe_to_markdown(
    df,
    file_name="dataframe_table.md",
    highlight_rows=None,
    center_align_columns=None,
    column_widths=100,
):
    """
    Convert a Pandas DataFrame to a custom Markdown table, highlight specific rows,
    right align specified columns, and save it to a file, with the first column always
    left-aligned in both header and data.

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
        md_output += f'<th style="text-align:{header_align}; width: {column_widths}px;"><strong>{col}</strong></th>\n'
    md_output += "</tr>\n</thead>\n<tbody>\n"

    # Add the table rows
    for index, row in df.iterrows():
        md_output += "<tr>\n"
        for i, col in enumerate(df.columns):
            cell_value = "" if pd.isna(row[col]) else row[col]

            # Determine the alignment based on the column name and index
            if i == 0:
                align = "left"  # Left align for the first column
            elif col in center_align_columns:
                align = "center"  # Right align for specified columns
            else:
                align = "right"  # Center align for other columns

            # Apply highlight if the row index is in the highlight_rows list
            if index in highlight_rows:
                md_output += f'<td style="text-align:{align}"><strong>{cell_value}</strong></td>\n'
            else:
                md_output += f'<td style="text-align:{align}">{cell_value}</td>\n'
        md_output += "</tr>\n"

    md_output += "</tbody>\n</table>"

    # Save to a Markdown file
    with open(file_name, "w") as file:
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
    formatted_df = df.fillna("-")

    # Format specified numeric columns
    for col in numeric_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_numeric(x))

    # Format percentage columns
    for col in percentage_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_percentage(x))

    return formatted_df


def format_numeric(x):
    """Format a numeric value with commas and no decimal places."""
    try:
        return f"{float(x):,.0f}" if x not in ["-", ""] else x
    except ValueError:
        return x


def format_percentage(x):
    """Format a value as a percentage."""
    try:
        return f"{float(x):.0f}%" if x not in ["-", ""] else x
    except ValueError:
        return x


def convert_to_integer(value):
    try:
        return int(value)
    except ValueError:
        return value


# Get Observed data from NTD
# Read the Excel file, assuming headers are not in the first row
df = df = pd.read_excel(
    observed_NTD, usecols="B:D", skiprows=list(range(4)), header=None, engine="openpyxl"
)

# A list to hold all the tables as DataFrames
tables = []

# A variable to keep track of the start of the current table
current_table_start = None

# Identifying header rows based on specific column content
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
    # table['Annual UPT'] = table['Annual UPT'].apply(lambda x: format_numeric(x))
    table["% of total ridership"] = table["% of total ridership"].apply(
        lambda x: f"{x:.2%}" if not pd.isna(x) else x
    )
    table = table.reset_index(drop=True)
    tables[i] = table

# Assuming 'tables' is a list of DataFrames
new_df_data = []  # Prepare an empty list to store the data

for df in tables:
    # Extract operator name - assuming it's the first cell of the first column in each table
    operator = df.columns[0]

    # Find 'Average Weekday UPT' if it exists, otherwise use 'Total Annual UPT'/260
    if "Average Weekday UPT" in df.iloc[:, 0].values:
        average_upt = df[df.iloc[:, 0] == "Average Weekday UPT"].iloc[0, 1]
    else:
        total_annual_upt = df[df.iloc[:, 0] == "Total Annual UPT"].iloc[0, 1]
        average_upt = (
            total_annual_upt / 260
        )  # Fallback if 'Average Weekday UPT' is not found

    # Append the result
    new_df_data.append([operator, average_upt])

# Convert the list to a DataFrame
new_df = pd.DataFrame(new_df_data, columns=["Operator", "Average Weekday UPT"])
new_df = new_df.rename(columns={"Average Weekday UPT": "Observed"})
observal_operator = new_df.copy()


AC_Transbay = tables[0][tables[0]["AC-Transit"] == "Commuter Bus"]["Annual UPT"] / 261
AC_Eastbay = tables[0][tables[0]["AC-Transit"] == "Bus"]["Annual UPT"] / 261
AC = {
    "Operator": ["AC Transbay", "AC Eastbay"],
    "Observed": [AC_Transbay.mean(), AC_Eastbay.mean()],
}
df_AC = pd.DataFrame(AC)

GGT_bus = tables[6][tables[6]["GG Transit"] == "Bus"]["Annual UPT"] / 261
GGT_Ferry = tables[6][tables[6]["GG Transit"] == "Ferryboat"]["Annual UPT"] / 261
GG = {
    "Operator": ["GGT-Bus", "GGT-Ferry"],
    "Observed": [GGT_bus.mean(), GGT_Ferry.mean()],
}
GG_Transit = pd.DataFrame(GG)

MUNI_Bus = tables[8][tables[8]["MUNI"] == "Bus"]["Annual UPT"] / 261
MUNI_Rail = tables[8][tables[8]["MUNI"] == "Light Rail"]["Annual UPT"] / 261
MUNI_Cable = tables[8][tables[8]["MUNI"] == "Cable Car"]["Annual UPT"] / 261
MUNI = {
    "Operator": ["MUNI-Bus", "MUNI-Rail", "MUNI_Cable"],
    "Observed": [MUNI_Bus.mean(), MUNI_Rail.mean(), MUNI_Cable.mean()],
}
MUNI = pd.DataFrame(MUNI)

SCVTA_bus = tables[14][tables[14]["SCVTA"] == "Bus"]["Annual UPT"] / 261
SCVTA_rail = tables[14][tables[14]["SCVTA"] == "Light Rail"]["Annual UPT"] / 261
SCVTA = {
    "Operator": ["SCVTA-Bus", "SCVTA-LRT"],
    "Observed": [SCVTA_bus.mean(), SCVTA_rail.mean()],
}
SCVTA = pd.DataFrame(SCVTA)

# All observed data
total = pd.concat([new_df, df_AC, GG_Transit, MUNI, SCVTA], ignore_index=True)
df_filtered = total.loc[~total["Operator"].isin(["MUNI", "GG Transit", "SCVTA"])]
df_filtered = df_filtered.sort_values(by="Operator", ascending=True)

name_mapping = {
    "AC Eastbay": "AC Eastbay",
    "AC Transbay": "AC Transbay",
    "AC-Transit": "AC-Transit",
    "ACE": "ACE",
    "BART": "BART",
    "CCCTA": "CCCTA",
    "CalTrain": "CalTrain",
    "FAST": "FAST",
    "GGT-Bus": "GGT-Bus",
    "GGT-Ferry": "GGT-Ferry",
    "LAVTA-Wheels": "LAVTA",
    "MUNI-Bus": "Muni - Bus",
    "MUNI-Rail": "Muni - Rail",
    "MUNI_Cable": "Muni - Cable",
    "Napa Vine": "VINE",
    "Petaluma Transit": "Petaluma",
    "Rio Vista Delta Breeze": "Rio Vista Delta Breeze ",
    "SCVTA-Bus": "SCVTA-Bus",
    "SCVTA-LRT": "SCVTA-LRT",
    "SF Bay Ferry": "SF Bay Ferry (WETA)",
    "SMART": "SMART",
    "SamTrans": "SamTrans",
    "Santa Rosa CityBus": "Santa Rosa",
    "SolTrans": "SolTrans",
    "Sonoma County Transit": "Sonoma County Transit",
    "Tri-Delta": "Tri Delta Transit",
    "Union City": "Union City Transit",
    "Vacaville City Coach": "Vacaville City Coach",
    "WestCat": "WestCat",
}

data_frames = []  # List to collect DataFrames

for path in transit_assignments:
    df = read_dbf_and_groupby_sum(path, ["SYSTEM"], "AB_BRDA")
    data_frames.append(df)

all = pd.concat(data_frames, ignore_index=True)
all = all.groupby("SYSTEM")["AB_BRDA"].sum().reset_index()
all = all.rename(columns={"SYSTEM": "Operator", "AB_BRDA": "Modeled"})
model_operator = all.copy()

data_frames_mode = []  # List to collect DataFrames

for path in transit_assignments:
    df = read_dbf_and_groupby_sum(path, ["MODE"], "AB_BRDA")
    data_frames_mode.append(df)

all_mode = pd.concat(data_frames_mode, ignore_index=True)
all_mode = all_mode.groupby("MODE")["AB_BRDA"].sum().reset_index()
all_mode = all_mode.rename(columns={"MODE": "Operator", "AB_BRDA": "Modeled"})

GG_bus_model = all_mode[all_mode["Operator"] == 23]["Modeled"].mean()
VTA_LR_model = all_mode[all_mode["Operator"] == 21]["Modeled"].mean()
VTA_bus_model = all_mode[all_mode["Operator"] == 19]["Modeled"].mean()


# Get Ferry data
def assign_ferry_name(name):
    transbay = [
        "90_FAOF",
        "90_FBHB",
        "90FOAF",
        "90_HBFB",
        "90_OAFW",
        "90_OAKOP",
        "90_OPOAK",
        "90_WFAO",
    ]
    GG_transit = [
        "91_LARKN",
        "91_LARKS",
        "92_FBSAU",
        "92_SAUFB",
        "93_FBTIB",
        "93_FWSAU",
        "93_FWTIB",
        "93_SAUFW",
        "93_TIBFB",
        "93_TIBFW",
    ]
    if name in transbay:
        return "SF Bay Ferry"
    elif name in GG_transit:
        return "GGT-Ferry"
    else:
        return "Other"


def ferry_total(dbf_filepath, filter_columns, sum_column):
    # TODO very much similar logic to transit.read_dbf_and_groupby_sum()
    # simplify/merge also with muni_total and ac_total
    gdf = gpd.read_file(dbf_filepath)
    # Filter the DataFrame based on the 'SYSTEM' column
    filtered_df = gdf[gdf["SYSTEM"] == filter_columns]
    filtered_df = filtered_df[["A", "B", "NAME", sum_column]]
    # Apply the function to each row in the DataFrame to create the new column
    filtered_df["Ferry_name"] = filtered_df["NAME"].apply(assign_ferry_name)
    filtered_df = filtered_df.groupby("Ferry_name")["AB_BRDA"].sum().reset_index()
    return filtered_df


ferry_df = []  # List to collect DataFrames

for path in transit_assignments:
    df = ferry_total(path, "Ferry", "AB_BRDA")
    ferry_df.append(df)

ferry = pd.concat(ferry_df, ignore_index=True)
ferry = ferry.groupby("Ferry_name")["AB_BRDA"].sum().reset_index()

model_GG_ferry = ferry[ferry["Ferry_name"] == "GGT-Ferry"]["AB_BRDA"].mean()
model_SF_Bay = ferry[ferry["Ferry_name"] == "SF Bay Ferry"]["AB_BRDA"].mean()


def assign_muni_name(name):
    cable = ["Cable Car"]
    rail = ["LRV1", "LRV2"]
    if name in cable:
        return "Cable Car"
    elif name in rail:
        return "Rail"
    else:
        return "Bus"


def muni_total(dbf_filepath):
    # TODO very much similar logic to transit.read_dbf_and_groupby_sum()
    # simplify/merge also with ferry_total and ac_total
    gdf = gpd.read_file(dbf_filepath)
    # Filter the DataFrame based on the 'SYSTEM' column
    filtered_df = gdf[gdf["SYSTEM"] == "SF MUNI"]
    filtered_df = filtered_df[["A", "B", "VEHTYPE", "AB_BRDA"]]
    # Apply the function to each row in the DataFrame to create the new column
    filtered_df["MUNI_name"] = filtered_df["VEHTYPE"].apply(assign_muni_name)
    filtered_df = filtered_df.groupby("MUNI_name")["AB_BRDA"].sum().reset_index()

    return filtered_df


muni_df = []  # List to collect DataFrames

for path in transit_assignments:
    df = muni_total(path)
    muni_df.append(df)

muni = pd.concat(muni_df, ignore_index=True)
muni = muni.groupby("MUNI_name")["AB_BRDA"].sum().reset_index()
model_muni_bus = all_mode[all_mode["Operator"] == 11]["Modeled"].mean()
model_muni_cable = muni[muni["MUNI_name"] == "Cable Car"]["AB_BRDA"].mean()
model_muni_rail = all_mode[all_mode["Operator"] == 15]["Modeled"].mean()


def ac_total(dbf_filepath, system):
    # TODO very much similar logic to transit.read_dbf_and_groupby_sum()
    # simplify/merge also with muni_total and ferry_total
    gdf = gpd.read_file(dbf_filepath)
    # Filter the DataFrame based on the 'SYSTEM' column
    filtered_df = gdf[gdf["SYSTEM"] == system]
    filtered_df = filtered_df[["A", "B", "MODE", "AB_BRDA"]]
    # Apply the function to each row in the DataFrame to create the new column
    filtered_df = filtered_df.groupby("MODE")["AB_BRDA"].sum().reset_index()
    return filtered_df


ac_df = []  # List to collect DataFrames

for path in transit_assignments:
    df = ac_total(path, "AC Transit")
    ac_df.append(df)

ac = pd.concat(ac_df, ignore_index=True)
ac = ac.groupby("MODE")["AB_BRDA"].sum().reset_index()
ac_trans = ac[ac["MODE"] == 22]["AB_BRDA"].mean()
ac_east = ac[ac["MODE"] == 18]["AB_BRDA"].mean()

model_dic = {
    "Operator": [
        "GGT-Ferry",
        "GGT-Bus",
        "SCVTA-Bus",
        "SCVTA-LRT",
        "SF Bay Ferry (WETA)",
        "Muni-Bus",
        "Muni-rail",
        "Muni-Cable",
        "AC Transbay",
        "AC Eastbay",
    ],
    "Modeled": [
        model_GG_ferry,
        GG_bus_model,
        VTA_bus_model,
        VTA_LR_model,
        model_SF_Bay,
        model_muni_bus,
        model_muni_rail,
        model_muni_cable,
        ac_trans,
        ac_east,
    ],
}
df_model_dic = pd.DataFrame(model_dic)

df_modeled = pd.concat([all, df_model_dic])
df = pd.read_excel(
    Transit_Templet,
    usecols="C:D",
    sheet_name="val_Tot",
    skiprows=list(range(4)) + list(range(37, 51)),
)

name_mapping2 = {
    "AC Eastbay": "AC Eastbay",
    "AC Transbay": "AC Transbay",
    "AC Transit": "AC-Transit",
    "ACE": "ACE",
    "BART": "BART",
    "CCCTA": "CCCTA",
    "Caltrain": "CalTrain",
    "FAST": "FAST",
    "GGT-Bus": "GGT-Bus",
    "GGT-Ferry": "GGT-Ferry",
    "LAVTA": "LAVTA",
    "Muni-Bus": "Muni - Bus",
    "Muni-rail": "Muni - Rail",
    "Muni-Cable": "Muni - Cable",
    "VINE": "VINE",
    "Petaluma": "Petaluma",
    "SCVTA-Bus": "SCVTA-Bus",
    "SCVTA-LRT": "SCVTA-LRT",
    "SF Bay Ferry (WETA)": "SF Bay Ferry (WETA)",
    "SMART": "SMART",
    "SamTrans": "SamTrans",
    "Santa Rosa CityBus": "Santa Rosa",
    "SolTrans": "SolTrans",
    "Shuttle": "Standford Shuttles",
    "Sonoma County Transit": "Sonoma County Transit",
    "Tri Delta Transit": "Tri Delta Transit",
    "Union City Transit": "Union City Transit",
    "Vacaville": "Vacaville City Coach",
    "WestCAT": "WestCat",
}

name_mapping3 = {
    "Golden Gate Transit": "GG Transit",
    "SF MUNI": "MUNI",
}

df_sf_ferry = pd.DataFrame(
    {"Operator": ["SF Bay Ferry (WETA)"], "Modeled": [model_SF_Bay]}
)
model_operator = pd.concat([model_operator, df_sf_ferry])

observal_operator["Operator"] = (
    observal_operator["Operator"]
    .map(name_mapping)
    .fillna(observal_operator["Operator"])
)
model_operator["Operator"] = (
    model_operator["Operator"].map(name_mapping3).fillna(model_operator["Operator"])
)
df_operator = pd.merge(observal_operator, model_operator, on="Operator", how="left")
total_row_o = pd.Series(df_operator[["Observed", "Modeled"]].sum(), name="Total")
df_operator = pd.concat([df_operator, total_row_o.to_frame().T], ignore_index=True)
df_operator["Diff"] = df_operator["Modeled"] - df_operator["Observed"]
df_operator["Pct Diff"] = (df_operator["Diff"] / df_operator["Observed"]) * 100
df_operator.at[df_operator.index[-1], "Operator"] = "Total"
total_operator = format_dataframe(
    df_operator, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
)
dataframe_to_markdown(
    total_operator,
    file_name=os.path.join(OUTPUT_FOLDER, "valTotal_Operator2.md"),
    highlight_rows=[23],
    center_align_columns=None,
    column_widths=100,
)
total_operator[:-1].to_csv(
    os.path.join(total_output_dir, "valTotal_Operator2.csv"), index=False
)

df_filtered["Operator"] = (
    df_filtered["Operator"].map(name_mapping).fillna(df_filtered["Operator"])
)
df_modeled["Operator"] = (
    df_modeled["Operator"].map(name_mapping2).fillna(df_modeled["Operator"])
)
# Now perform the join, assuming you want to merge df1 and df2 based on the 'Operator' column
result_df = pd.merge(df, df_filtered, on="Operator", how="left")
result_df = pd.merge(result_df, df_modeled, on="Operator", how="left")
total_row = pd.Series(result_df[["Observed", "Modeled"]].sum(), name="Total")
result_df = pd.concat([result_df, total_row.to_frame().T], ignore_index=True)
result_df["Diff"] = result_df["Modeled"] - result_df["Observed"]
result_df["Pct Diff"] = (result_df["Diff"] / result_df["Observed"]) * 100
result_df.at[result_df.index[-1], "Operator"] = "Total"
result_df = result_df[result_df["Operator"] != "AC-Transit"]

result_csv = result_df.copy()
result_csv = result_csv[:-1]
result_csv = result_csv.dropna()
result_csv.to_csv(os.path.join(total_output_dir, "valTotal_Operator.csv"), index=False)

df_service_type = (
    result_df[["Service Type", "Observed", "Modeled"]]
    .groupby("Service Type")
    .sum()
    .reset_index()
)
st_total_row = pd.Series(df_service_type[["Observed", "Modeled"]].sum(), name="Total")
df_service_type = pd.concat(
    [df_service_type, st_total_row.to_frame().T], ignore_index=True
)
df_service_type["Diff"] = df_service_type["Modeled"] - df_service_type["Observed"]
df_service_type["Pct Diff"] = (
    df_service_type["Diff"] / df_service_type["Observed"]
) * 100
df_service_type.at[df_service_type.index[-1], "Service Type"] = "Total"
total_service = format_dataframe(
    df_service_type, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
)
dataframe_to_markdown(
    total_service,
    file_name=os.path.join(OUTPUT_FOLDER, "valTotal_Service.md"),
    highlight_rows=[5],
    center_align_columns=None,
    column_widths=100,
)
total_service[:-1].to_csv(
    os.path.join(total_output_dir, "valTotal_Service.csv"), index=False
)

total_val = format_dataframe(result_df, ["Observed", "Modeled", "Diff"], ["Pct Diff"])
dataframe_to_markdown(
    total_val,
    file_name=os.path.join(OUTPUT_FOLDER, "valTotal_Operator.md"),
    highlight_rows=[32],
    center_align_columns=["Operator"],
    column_widths=100,
)
