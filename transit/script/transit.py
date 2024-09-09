import subprocess
from pathlib import Path
import geopandas as gpd
import pandas as pd
import tomli as tomllib
from bart import process_BART_data, process_BART_county, process_BART_SL
from muni import process_muni
from screen import concat_final_SL
from map_data import process_muni_map, process_bart_map
from obs import process_obs_data

script_dir = Path(__file__).parent
toml_path = script_dir / "transit.toml"

with open(toml_path, "rb") as f:
    config = tomllib.load(f)

transit_line_rename_filepath = (
    Path(config["directories"]["resources"]) / config["transit"]["line_rename_filename"]
)

transit_validation_2019_alfaro_filepath = config["transit"][
    "transit_validation_2019_alfaro_filepath"
]
model_run_dir = Path(config["directories"]["model_run"])
MUNI_output_dir = Path(config["directories"]["MUNI_output_dir"])
SHP_file_dir = Path(config["directories"]["SHP_file_dir"])
Base_model_dir = Path(config["directories"]["Base_model_dir"])
WORKING_FOLDER = Path(config["directories"]["transit_input_dir"])
FREEFLOW_SHP = Base_model_dir / config["transit"]["FREEFLOW_SHP"]
BART_output_dir = Path(config["directories"]["BART_output_dir"])
observed_BART = WORKING_FOLDER / config["transit"]["observed_BART"]
observed_BART_county = Path(config["transit"]["observed_BART_county"])
observed_BART_SL = Path(config["transit"]["observed_BART_SL"])
observed_MUNI_Line = Path(config["transit"]["observed_MUNI_Line"])
observed_SL = Path(config["transit"]["observed_SL"])
observed_NTD = Path(config["transit"]["observed_NTD"])
output_dir = model_run_dir / "validation_workbook" / "output"
output_transit_dir = output_dir / "transit"
output_transit_dir.mkdir(parents=True, exist_ok=True)

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def transit_assignment_filepaths(
    model_run_dir=model_run_dir, time_periods=time_periods
):
    return {t: Path(model_run_dir) / f"SFALLMSA{t}.DBF" for t in time_periods}

def read_transit_assignments(model_run_dir, time_periods):
    """Reads the DBF files for each time period using Geopandas and concatenates them."""
    filepaths = transit_assignment_filepaths(model_run_dir, time_periods)
    
    # List to store all GeoDataFrames
    gdf_list = []
    
    for period, filepath in filepaths.items():
        try:
            # Read the DBF file using geopandas
            gdf = gpd.read_file(str(filepath))
            
            # Add a new column 'TOD' to represent the time period
            gdf['TOD'] = period
            
            # Append to the list
            gdf_list.append(gdf)
            
            print(f"Successfully read and added 'TOD' to: {filepath}")
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            raise NotImplementedError(f"Could not read or extract data from {filepath}")
    
    # Concatenate all GeoDataFrames in the list into a single DataFrame
    combined_gdf = gpd.pd.concat(gdf_list, ignore_index=True)
    
    return combined_gdf


# def read_dbf_and_groupby_sum(dbf_file, system_filter, groupby_columns, sum_column):
#     """
#     Reads a DBF file, filters by SYSTEM, group by specified columns,
#     and calculates sum of a specified column.

#     Parameters:
#     dbf_file_path (str): The path to the DBF file.
#     system_filter (str): The value to filter by on the 'SYSTEM' column.
#     groupby_columns (list): The list of columns to group by.
#     sum_column (str): The column on which to calculate the sum.

#     Returns:
#     DataFrame: Pandas DataFrame with the groupby and sum applied.
#     """
#     filtered_df = dbf_file[dbf_file["SYSTEM"] == system_filter]  # filter on SYSTEM columns
#     # group by `groupby_columns` and sum `sum_column`
#     grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()
#     # reset index to convert it back to a DataFrame
#     grouped_sum_df = grouped_sum.reset_index()
#     return grouped_sum_df
        

def out_put_data():
    combined_gdf = read_transit_assignments(model_run_dir, time_periods)
    process_BART_data(combined_gdf, model_run_dir, output_transit_dir)
    process_BART_county(combined_gdf, model_run_dir, output_transit_dir)
    process_BART_SL(combined_gdf, model_run_dir, output_transit_dir)
    process_muni(combined_gdf, model_run_dir, transit_line_rename_filepath, transit_validation_2019_alfaro_filepath, output_transit_dir)
    concat_final_SL(combined_gdf, output_transit_dir)
    process_muni_map(combined_gdf,output_transit_dir, MUNI_output_dir,SHP_file_dir,FREEFLOW_SHP)
    process_bart_map(BART_output_dir, output_transit_dir, observed_BART)
    process_obs_data(observed_MUNI_Line, output_transit_dir, observed_BART, observed_BART_county, observed_BART_SL, observed_SL, observed_NTD)
    
    
out_put_data()




# if __name__ == "__main__":
#     # TODO call functions from each script directly rather than use subprocess
#     # make sure that there are no circular imports by passing data structures directly
#     # rather than importing the shared read functions from here to the scripts
#     scripts = [
#         "bart.py",
#         "muni.py",
#         "screen.py",
#         "simwrapper_table.py",
#         "map_data.py",
#         "obs.py",
#         "total_val.py",
#     ]
#     for script_name in scripts:
#         print(f"Running {script_name}")
#         result = subprocess.run(["python", script_name], capture_output=True, text=True)
#         if result.returncode == 0:
#             print(f"Successfully ran {script_name}")
#         else:
#             print(f"Error running {script_name}: {result.stderr}")
