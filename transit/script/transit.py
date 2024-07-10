import subprocess
from pathlib import Path

import geopandas as gpd

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def transit_assignment_filepaths(model_run_dir, time_periods=time_periods):
    model_run_dir = Path(model_run_dir)
    return [model_run_dir / f"SFALLMSA{t}.DBF" for t in time_periods]


def read_transit_assignments(model_run_dir, time_periods=time_periods):
    filepaths = transit_assignment_filepaths(model_run_dir, time_periods=time_periods)
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
    scripts = [
        "BART.py",
        "MUNI.py",
        "Screen.py",
        "Simwrapper_table.py",
        "map_data.py",
        "Obs.py",
        "total_val.py",
    ]

    for script_name in scripts:
        print(f"Running {script_name}")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully ran {script_name}")
        else:
            print(f"Error running {script_name}: {result.stderr}")
