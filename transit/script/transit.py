import subprocess
from pathlib import Path
import geopandas as gpd
import tomli as tomllib
from bart import process_BART_data, process_BART_county, process_BART_SL
from muni import process_muni
from screen import concat_final_SL

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
output_dir = model_run_dir / "validation_workbook" / "output"
output_transit_dir = output_dir / "transit"
output_transit_dir.mkdir(parents=True, exist_ok=True)

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def transit_assignment_filepaths(
    model_run_dir=model_run_dir, time_periods=time_periods
):
    return {t: Path(model_run_dir) / f"SFALLMSA{t}.DBF" for t in time_periods}

# def read_transit_assignments(model_run_dir, time_periods):
#     """Reads the DBF files for each time period using Geopandas and concatenates them."""
#     filepaths = transit_assignment_filepaths(model_run_dir, time_periods)
    
#     # List to store all GeoDataFrames
#     gdf_list = []
    
#     for period, filepath in filepaths.items():
#         try:
#             # Read the DBF file using geopandas
#             gdf = gpd.read_file(str(filepath))
            
#             # Add a new column 'TOD' to represent the time period
#             gdf['TOD'] = period
            
#             # Append to the list
#             gdf_list.append(gdf)
            
#             print(f"Successfully read and added 'TOD' to: {filepath}")
#         except Exception as e:
#             print(f"Error reading {filepath}: {e}")
#             raise NotImplementedError(f"Could not read or extract data from {filepath}")
    
#     # Concatenate all GeoDataFrames in the list into a single DataFrame
#     combined_gdf = gpd.pd.concat(gdf_list, ignore_index=True)
    
#     return combined_gdf
        

# def out_put_data():
#     combined_gdf = read_transit_assignments(model_run_dir, time_periods)
#     process_BART_data(combined_gdf, model_run_dir, output_transit_dir)
#     process_BART_county(combined_gdf, model_run_dir, output_transit_dir)
#     process_BART_SL(combined_gdf, model_run_dir, output_transit_dir)
#     process_muni(combined_gdf, model_run_dir, transit_line_rename_filepath, transit_validation_2019_alfaro_filepath, output_transit_dir)
#     concat_final_SL(combined_gdf, output_transit_dir)
    
# out_put_data()




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
