from pathlib import Path

# import numpy as np
import pandas as pd
import tomllib
from transit.utils import read_dbf_and_groupby_sum, read_transit_assignments, time_periods


def map_name_to_direction(name):
    if name.endswith("I"):
        return "IB"
    elif name.endswith("O"):
        return "OB"
    else:
        return None  # Return None for other cases


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
        combined_gdf, "SF MUNI", ["FULLNAME", "NAME", "TOD"], "AB_BRDA"
    )
    MUNI["Direction"] = MUNI["NAME"].apply(map_name_to_direction)

    MUNI = MUNI.sort_values(by="NAME").reset_index(drop=True)
    # Merge df1 with df2 based on the Name/NAME column
    df_merged = pd.merge(MUNI, rename[["NAME", "New"]], on="NAME", how="left")

    # Update the FULLNAME in df1 with the New value from df2 if a match is found
    df_merged["FULLNAME"] = df_merged["New"].combine_first(df_merged["FULLNAME"])
    df_merged = df_merged.drop(columns=["New"])
    # MUNI = MUNI.rename(columns={"NAME": "Name", "AB_BRDA": "Ridership"})

    MUNI_full = pd.merge(df_merged, obs_model_name_match, on="FULLNAME", how="left")
    MUNI_full = MUNI_full.rename(
        columns={"NAME": "Name", "AB_BRDA": "Ridership", "obs_line": "Line"}
    )

    # Apply the transformation function to the 'Line' column
    obs_MUNI_line = pd.read_csv(transit_input_dir / observed_MUNI_Line)
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
