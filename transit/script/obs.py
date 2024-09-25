from pathlib import Path

import pandas as pd
import tomllib
from transit_function import (
    dataframe_to_markdown,
    format_numeric,
    read_transit_assignments,
)


def process_obs_data(
    transit_input_dir,
    markdown_output_dir,
    observed_MUNI_Line,
    observed_BART,
    observed_BART_county,
    observed_BART_SL,
    observed_SL,
    observed_NTD,
    obs_MUNI_line_md,
    obs_BART_station_md,
    obs_BART_county_md,
    obs_BART_SL_md,
    obs_Screenlines_md,
    obs_NTD_md,
):
    obs_MUNI_line = pd.read_csv(transit_input_dir / observed_MUNI_Line)
    obs_MUNI_line["Ridership"] = obs_MUNI_line["Ridership"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_MUNI_line,
        Path(markdown_output_dir / obs_MUNI_line_md),
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

    obs_BART_line = pd.read_csv(transit_input_dir / observed_BART)
    obs_BART_line["Boardings"] = obs_BART_line["Boardings"].apply(
        lambda x: format_numeric(x)
    )
    obs_BART_line["Alightings"] = obs_BART_line["Alightings"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_BART_line,
        Path(markdown_output_dir / obs_BART_station_md),
        highlight_rows=None,
        center_align_columns=["TOD", "Key"],
        column_widths=100,
    )

    obs_BART_county = pd.read_csv(transit_input_dir / observed_BART_county)
    obs_BART_county["Boardings"] = obs_BART_county["Boardings"].apply(
        lambda x: format_numeric(x)
    )
    obs_BART_county["Alightings"] = obs_BART_county["Alightings"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_BART_county,
        Path(markdown_output_dir / obs_BART_county_md),
        highlight_rows=None,
        center_align_columns=["TOD", "Key"],
        column_widths=100,
    )

    obs_BART_SL = pd.read_csv(transit_input_dir / observed_BART_SL)
    obs_BART_SL["Ridership"] = obs_BART_SL["Ridership"].apply(
        lambda x: format_numeric(x)
    )
    dataframe_to_markdown(
        obs_BART_SL,
        Path(markdown_output_dir / obs_BART_SL_md),
        highlight_rows=None,
        center_align_columns=["Direction", "TOD", "Key"],
        column_widths=100,
    )

    obs_SL = pd.read_csv(transit_input_dir / observed_SL)
    obs_SL["Ridership"] = obs_SL["Ridership"].apply(lambda x: format_numeric(x))
    dataframe_to_markdown(
        obs_SL,
        Path(markdown_output_dir / obs_Screenlines_md),
        highlight_rows=None,
        center_align_columns=["Direction", "TOD", "Key", "Operator", "Mode"],
        column_widths=100,
    )

    obs_NTD_df = pd.read_csv(transit_input_dir / observed_NTD)
    dataframe_to_markdown(
        obs_NTD_df,
        Path(markdown_output_dir / obs_NTD_md),
        highlight_rows=None,
        center_align_columns=None,
        column_widths=100,
    )


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    transit_input_dir = config["directories"]["transit_input_dir"]
    markdown_output_dir = config["directories"]["markdown_output_dir"]
    model_run_dir = config["directories"]["model_run"]
    observed_BART = config["transit"]["observed_BART"]
    observed_MUNI_Line = config["transit"]["observed_MUNI_Line"]
    observed_BART_county = config["transit"]["observed_BART_county"]
    observed_BART_SL = config["transit"]["observed_BART_SL"]
    observed_MUNI_Line = config["transit"]["observed_MUNI_Line"]
    observed_SL = config["transit"]["observed_SL"]
    observed_NTD = config["transit"]["observed_NTD"]
    model_BART = config["output"]["model_BART"]
    model_BART_county = config["output"]["model_BART_county"]
    model_BART_SL = config["output"]["model_BART_SL"]
    obs_MUNI_line_md = config["output"]["obs_MUNI_line_md"]
    obs_BART_station_md = config["output"]["obs_BART_station_md"]
    obs_BART_county_md = config["output"]["obs_BART_county_md"]
    obs_BART_SL_md = config["output"]["obs_BART_SL_md"]
    obs_Screenlines_md = config["output"]["obs_Screenlines_md"]
    obs_NTD_md = config["output"]["obs_NTD_md"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    dbf_file = read_transit_assignments(model_run_dir, time_periods)

    process_obs_data(
        transit_input_dir,
        markdown_output_dir,
        observed_MUNI_Line,
        observed_BART,
        observed_BART_county,
        observed_BART_SL,
        observed_SL,
        observed_NTD,
        obs_MUNI_line_md,
        obs_BART_station_md,
        obs_BART_county_md,
        obs_BART_SL_md,
        obs_Screenlines_md,
        obs_NTD_md,
    )
