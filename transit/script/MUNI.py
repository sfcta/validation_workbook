import os
import tomllib

import numpy as np
import pandas as pd
import shapefile

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)
WORKING_FOLDER = config["directories"]["transit_input_dir"]
OUTPUT_FOLDER = config["directories"]["transit_output_dir"]
AM_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAAM_DBF"])
PM_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAPM_DBF"])
MD_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAMD_DBF"])
EV_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAEV_DBF"])
EA_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAEA_DBF"])
Line_Name_File = os.path.join(WORKING_FOLDER, config["transit"]["Line_Name_File"])
Line_Rename_File = os.path.join(WORKING_FOLDER, config["transit"]["Line_Rename_File"])
Transit_Templet = os.path.join(WORKING_FOLDER, config["transit"]["Transit_Templet"])
files_path = [AM_dbf, PM_dbf, MD_dbf, EV_dbf, EA_dbf]

file_check = [
    WORKING_FOLDER,
    AM_dbf,
    PM_dbf,
    MD_dbf,
    EV_dbf,
    EA_dbf,
    Line_Name_File,
    Line_Rename_File,
    Transit_Templet,
]
for path in file_check:
    if not os.path.exists(path):
        print(f"{path}: Not Exists")


def read_dbf_and_groupby_sum(dbf_file_path, system_filter, groupby_columns, sum_column):
    """
    Reads a DBF file, filters by system, groups by specified columns,
    and calculates sum of a specified column.

    Parameters:
    dbf_file_path (str): The path to the DBF file.
    system_filter (str): The value to filter by on the 'SYSTEM' column.
    groupby_columns (list): The list of columns to group by.
    sum_column (str): The column on which to calculate the sum.

    Returns:
    DataFrame: Pandas DataFrame with the groupby and sum applied.
    """
    # Create a shapefile reader object
    sf = shapefile.Reader(dbf_file_path)

    # Extract fields and records from the DBF file
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()

    # Create a DataFrame using the extracted data
    df = pd.DataFrame(columns=fields, data=records)

    # Filter the DataFrame based on the 'SYSTEM' column
    filtered_df = df[df["SYSTEM"] == system_filter]

    # Group by the specified columns and sum the specified column
    grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()

    # Resetting index to convert it back to a DataFrame
    grouped_sum_df = grouped_sum.reset_index()

    return grouped_sum_df  # Define a function to map 'NAME' to 'Direction'


def map_name_to_direction(name):
    if name.endswith("I"):
        return "IB"
    elif name.endswith("O"):
        return "OB"
    else:
        return None  # Return None for other cases


MUNI = []  # List to collect DataFrames

for path in files_path:
    period = path[-6:-4]
    df = read_dbf_and_groupby_sum(path, "SF MUNI", ["FULLNAME", "NAME"], "AB_BRDA")
    df["Direction"] = df["NAME"].apply(map_name_to_direction)
    df["TOD"] = period
    MUNI.append(df)

MUNI_Day = pd.concat(MUNI)
MUNI_Day = MUNI_Day.sort_values(by="FULLNAME").reset_index(drop=True)
MUNI_Day = MUNI_Day.rename(columns={"NAME": "Name", "AB_BRDA": "Ridership"})

line_name = pd.read_csv(Line_Name_File)
line_name = line_name[line_name["System"] == "SF MUNI"]
line_name = line_name[["Name", "Line"]]

MUNI_full = pd.merge(MUNI_Day, line_name, on="Name", how="left")
rename = pd.read_csv(Line_Rename_File)
rename["new_name"] = rename["New"].str.extract(r"(\d+[A-Za-z]*)")
name_to_trn_asgn_new = pd.Series(
    rename.Trn_asgn_new.values, index=rename.NAME
).to_dict()
new_name_to_line = pd.Series(
    rename.new_name.values, index=rename.Trn_asgn_new
).to_dict()

# Map the Name in df1 to Trn_asgn_new using the mapping
MUNI_full["Name"] = (
    MUNI_full["Name"].map(name_to_trn_asgn_new).fillna(MUNI_full["Name"])
)
MUNI_full["Line"] = MUNI_full["Name"].map(new_name_to_line).fillna(MUNI_full["Line"])


def transform_line(line):
    if pd.isna(line):
        return np.nan
    elif line.isdigit():  # Case 1: Only numbers
        return int(line)
    elif any(char.isdigit() for char in line) and any(
        char.isalpha() for char in line
    ):  # Case 2: Numbers and letters
        if "SHORT" in line:
            line = line.replace("SHORT", "R")
        if len(line) < 4:
            line = line.zfill(4)
        return line
    elif line.isalpha():  # Case 3: Only letters
        special_names = {
            "J": "J-Church ",
            "K": "KT-Ingleside/Third Street ",
            "L": "L-Taraval ",
            "M": "M-Ocean View ",
            "N": "N-Judah ",
        }
        return special_names.get(line, line)
    else:
        return line


# Apply the transformation function to the 'Line' column
MUNI_full["Line"] = MUNI_full["Line"].apply(transform_line)
obs_MUNI_line = pd.read_excel(
    Transit_Templet, usecols="B:H", sheet_name="obs_MUNI_line", skiprows=list(range(9))
)
mode = obs_MUNI_line[["Line", "Mode"]].drop_duplicates().reset_index(drop=True)
mode_dict = mode.set_index("Line")["Mode"].to_dict()
MUNI_full["Mode"] = MUNI_full["Line"].map(mode_dict)
MUNI_full["Key_line_dir"] = MUNI_full["Line"].astype(str) + MUNI_full["Direction"]
MUNI_full["Key_line_tod"] = (
    MUNI_full["Line"].astype(str) + MUNI_full["TOD"] + MUNI_full["Direction"]
)
MUNI_full = MUNI_full[
    [
        "Line",
        "Mode",
        "Direction",
        "TOD",
        "Key_line_dir",
        "Key_line_tod",
        "Ridership",
        "Name",
    ]
]
MUNI_full = MUNI_full.sort_values(by=["Line", "Direction", "TOD"]).reset_index(
    drop=True
)
MUNI_full.to_csv(os.path.join(OUTPUT_FOLDER, "model_MUNI_Line.csv"), index=False)
