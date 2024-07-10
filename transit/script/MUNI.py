import os
import tomllib
from pathlib import Path

import numpy as np
import pandas as pd

# we should be importing functions in this file into transit.py instead
# HOTFIX TODO pass results of read_transit_assignments() directly as arg
from transit import read_dbf_and_groupby_sum, transit_assignment_filepaths


def read_transit_lines(model_run_dir, transit_line_rename_filepath):
    line_names = pd.read_csv(
        Path(model_run_dir) / "transitLineToVehicle.csv",
        usecols=["Name", "Line", "System"],
    )
    line_names = line_names[line_names["System"] == "SF MUNI"]
    line_names = line_names[["Name", "Line"]]

    # TODO simplify this logic; it shouldn't require such computationally intensive
    # creation of Serieses and then dicts... Seems like a polars join then using
    # when/then/otherwise would be much simpler and efficient
    rename = pd.read_csv(transit_line_rename_filepath)
    rename["new_name"] = rename["New"].str.extract(r"(\d+[A-Za-z]*)")
    name_to_trn_asgn_new = pd.Series(
        rename.Trn_asgn_new.values, index=rename.NAME
    ).to_dict()
    new_name_to_line = pd.Series(
        rename.new_name.values, index=rename.Trn_asgn_new
    ).to_dict()

    # Map the Name & Line in line_names to transit_line_rename using the mappings
    line_names["Name"] = (
        line_names["Name"].map(name_to_trn_asgn_new).fillna(line_names["Name"])
    )
    line_names["Line"] = (
        line_names["Name"].map(new_name_to_line).fillna(line_names["Line"])
    )
    return line_names


with open("transit.toml", "rb") as f:
    config = tomllib.load(f)

model_run_dir = config["directories"]["model_run"]
transit_assignments = transit_assignment_filepaths(model_run_dir)
transit_line_rename_filepath = (
    config["directories"]["resources"] / config["transit"]["line_rename_filename"]
)
line_names = read_transit_lines(model_run_dir, transit_line_rename_filepath)

WORKING_FOLDER = Path(config["directories"]["transit_input_dir"])
OUTPUT_FOLDER = config["directories"]["transit_output_dir"]
Transit_Templet = WORKING_FOLDER / config["transit"]["Transit_Templet"]


def map_name_to_direction(name):
    if name.endswith("I"):
        return "IB"
    elif name.endswith("O"):
        return "OB"
    else:
        return None  # Return None for other cases


MUNI = []  # List to collect DataFrames

for path in transit_assignments:
    period = path[-6:-4]
    df = read_dbf_and_groupby_sum(path, "SF MUNI", ["FULLNAME", "NAME"], "AB_BRDA")
    df["Direction"] = df["NAME"].apply(map_name_to_direction)
    df["TOD"] = period
    MUNI.append(df)

MUNI_Day = pd.concat(MUNI)
MUNI_Day = MUNI_Day.sort_values(by="FULLNAME").reset_index(drop=True)
MUNI_Day = MUNI_Day.rename(columns={"NAME": "Name", "AB_BRDA": "Ridership"})

MUNI_full = pd.merge(MUNI_Day, line_names, on="Name", how="left")


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
