# Transit Model Validation Scripts

## Setup Instructions

Before running the scripts, configure the paths to all necessary input and output files in the control file as described below.

### Input Configuration

Specify the paths for the required input files under the `[transit]` section in the control file:

- `SFALLMSAAM.DBF`
- `SFALLMSAPM.DBF`
- `SFALLMSAMD.DBF`
- `SFALLMSAEV.DBF`
- `SFALLMSAEA.DBF`
- `nodes.xls`
- `transitLineToVehicle.csv`
- `transit_line_rename.csv`
- `Transit_Validation_2019 - MA.xlsx`

These files should be located in your specified `transit_input_dir`.

### Output Configuration

Specify directories for the output files where the transformed and predicted data will be saved:

- **Markdown Output Directory:** Stores markdown tables.
- **MUNI Output Directory:** Stores MUNI validation outputs.
- **BART Output Directory:** Stores BART validation outputs.
- **Screenline Output Directory:** Stores screenline validation outputs.
- **Total Output Directory:** Stores all validation outputs.

Change the root directory path (e.g., `X:\Projects\Miscellaneous\validation_simwrapper\transit\`) as necessary. The script will automatically create required folders and subfolders.

## Running the Scripts

1. Ensure all input files are placed in the specified `transit_input_dir`.
2. Run `transit.py` to execute the script.
3. Check the specified output directories for results.

Ensure all dashboard YAML files are placed in the `transit` folder.

For issues or further configuration needs, refer to the control file comments or submit an issue on this repository.
