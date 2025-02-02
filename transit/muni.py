from pathlib import Path

# import numpy as np
import pandas as pd
import toml
from transit.utils import read_dbf_and_groupby_sum, read_transit_assignments, time_periods


def map_name_to_direction(name):
    if name.endswith("I") or name in ["MUN61R"]:
        return "IB"
    elif name.endswith("O") or name in ["MUN61"]:
        return "OB"
    else:
        return None  # Return None for other cases

def routeType(x):
    if x in ['J-Church', 'KT-Ingleside/Third Street','M-Ocean View','N-Judah','T-Third Street']:
        return 'Rail'
    elif x in ['59', '60', '61', '61R']:
        return 'Cable Car'
    elif x in ['F-Market & Wharves']:
        return 'Streetcar'
    elif 'X' in str(x) or x in ['8']:
        return 'Express Bus'
    elif 'R' in str(x) and x not in ['94R']:
        return 'Rapid'  # Perhaps change to Rapid later
    else:
        return 'Local Bus'


def process_muni(
    combined_gdf,
    muni_name_match,
    transit_line_rename_filepath,
    transit_input_dir,
    observed_MUNI_Line,
    output_transit_dir,
    model_MUNI_Line,
):
    # line_names = read_transit_lines(model_run_dir, transit_line_rename_filepath)
    rename = pd.read_csv(transit_line_rename_filepath)
    obs_model_name_match = pd.read_csv(transit_input_dir / muni_name_match)
    obs_model_name_match = obs_model_name_match[["obs_line", "Name"]]
    obs_model_name_match = obs_model_name_match.rename(
        columns={"Name": "NAME"}
    )
    obs_model_name_match = obs_model_name_match.drop_duplicates()

    MUNI = read_dbf_and_groupby_sum(
        combined_gdf, "SF MUNI", ["NAME", "MODE","TOD"], "AB_BRDA"
    )
    MUNI["Direction"] = MUNI["NAME"].apply(map_name_to_direction)

    MUNI = MUNI.sort_values(by="NAME").reset_index(drop=True)
    # Merge df1 with df2 based on the Name/NAME column
    df_merged = pd.merge(MUNI, rename[["NAME", "Trn_asgn_new"]], on="NAME", how="left")
    # # Update the FULLNAME in df1 with the New value from df2 if a match is found
    df_merged["NAME"] = df_merged["Trn_asgn_new"].combine_first(df_merged["NAME"])
    df_merged = df_merged.drop(columns=["Trn_asgn_new"])
    # MUNI = MUNI.rename(columns={"NAME": "Name", "AB_BRDA": "Ridership"})

    MUNI_full = pd.merge(df_merged, obs_model_name_match, on="NAME", how="left")
    MUNI_full = MUNI_full.rename(
        columns={"NAME": "Name", "AB_BRDA": "Ridership", "obs_line": "Line"}
    )

    # Apply the transformation function to the 'Line' column
    # obs_MUNI_line = pd.read_csv(transit_input_dir / observed_MUNI_Line)
    # mode = obs_MUNI_line[["Line", "Mode"]].drop_duplicates().reset_index(drop=True)
    # mode_dict = mode.set_index("Line")["Mode"].to_dict()
    MUNI_full["Mode"] = MUNI_full["Line"].apply(lambda x: routeType(x))
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
    MUNI_full.to_csv(output_transit_dir / model_MUNI_Line, index=False)


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    process_muni(
        combined_gdf,
        muni_name_match,
        transit_line_rename_filepath,
        transit_input_dir,
        observed_MUNI_Line,
        output_transit_dir,
        model_MUNI_Line,
    )
