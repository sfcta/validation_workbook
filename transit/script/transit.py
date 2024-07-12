import subprocess
from pathlib import Path

import geopandas as gpd
import tomllib

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)

transit_line_rename_filepath = (
    Path(config["directories"]["resources"]) / config["transit"]["line_rename_filename"]
)
transit_validation_2019_alfaro_filepath = config["transit"][
    "transit_validation_2019_alfaro_filepath"
]
model_run_dir = Path(config["directories"]["model_run"])
output_dir = model_run_dir / "validation_workbook" / "output"
output_transit_dir = output_dir / "transit"
output_transit_dir.mkdir(parents=True, exist_ok=True)

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def transit_assignment_filepaths(
    model_run_dir=model_run_dir, time_periods=time_periods
):
    return {t: Path(model_run_dir) / f"SFALLMSA{t}.DBF" for t in time_periods}


def read_transit_assignments(model_run_dir=model_run_dir, time_periods=time_periods):
    # transit_assignment_filepaths(model_run_dir, time_periods=time_periods)
    # TODO consolidate from each subscript to this script & pass as function arg instead
    raise NotImplementedError


def read_dbf_and_groupby_sum(dbf_filepath, system_filter, groupby_columns, sum_column):
    """
    Reads a DBF file, filters by SYSTEM, group by specified columns,
    and calculates sum of a specified column.

    Parameters:
    dbf_file_path (str): The path to the DBF file.
    system_filter (str): The value to filter by on the 'SYSTEM' column.
    groupby_columns (list): The list of columns to group by.
    sum_column (str): The column on which to calculate the sum.

    Returns:
    DataFrame: Pandas DataFrame with the groupby and sum applied.
    """
    gdf = gpd.read_file(dbf_filepath)
    filtered_df = gdf[gdf["SYSTEM"] == system_filter]  # filter on SYSTEM columns
    # group by `groupby_columns` and sum `sum_column`
    grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()
    # reset index to convert it back to a DataFrame
    grouped_sum_df = grouped_sum.reset_index()
    return grouped_sum_df


if __name__ == "__main__":
    # TODO call functions from each script directly rather than use subprocess
    # make sure that there are no circular imports by passing data structures directly
    # rather than importing the shared read functions from here to the scripts
    scripts = [
        "bart.py",
        "muni.py",
        "screen.py",
        "simwrapper_table.py",
        "map_data.py",
        "obs.py",
        "total_val.py",
    ]
    for script_name in scripts:
        print(f"Running {script_name}")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully ran {script_name}")
        else:
            print(f"Error running {script_name}: {result.stderr}")
