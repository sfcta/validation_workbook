# Road Validation Documentation

This directory contains scripts and documentation for the validation of road models using the Simwrapper environment. Below you'll find details about each component, including the expected input, output, and configuration files.

## Contents

- [Overview](#overview)
- [Usage](#usage)
- [Scatter Plot (`scatter.py`)](#scatter-plot-scatterpy)
- [Statistical Metrics Calculation (`stats.py`)](#statistical-metrics-calculation-statspy)
- [Map Visualization (`map.py`)](#map-visualization-mappy)

## Overview

The validation process is controlled by a main script, `validation_road.py`, which orchestrates the execution of specific functionality scripts and uses a TOML configuration file for parameterization.

## Usage

1. **Configuration**:
   - Ensure the `config.toml` file is properly set up with the correct paths and parameters for your data and validation process.

2. **Execution**:    
    - To run the `validation_road.py` script with your configuration file, use the following command:

     ```bash
     python validation_road.py path/to/validation_road_config.toml
     ```

## Scatter Plot (`scatter.py`)

### Description
Generates scatter plots for road validation using time-specific loaded network files.

### Inputs
- **Loaded Network Files:** Requires 5 loaded network files for different time periods (`AM`, `MD`, `PM`, `EV`, `EA`).
- **Template Structure:** Observations should follow the structure outlined in the `Excel_File_Path` specified in the `config.toml` file (Loc Data tab).

### Outputs
- **JSON Files:** Two Vega-Lite configuration files for each grouping variable.
- **CSV:** `{chosen_time_period}_Scatter_data.csv` with all desired columns.

### Configuration
Edit specific titles and descriptions in the generated YAML file.

## Statistical Metrics Calculation (`stats.py`)

### Description
Calculates and exports several statistical metrics for road validation.

### Outputs
- **CSV Files:** Three concise CSV files (`percent_rmse`, `relative_error`, `est_obs_ratio`), and their "melted" versions. "Melted" refers to the process of transforming the data from a wide format (where each metric is a separate column) to a long format (where metrics are represented as values in a single column), which is often used for visualization purposes.
  
  - **Grouping Variables:** The metrics are typically grouped by categories such as location types (e.g., `Loc Type`, `AT Group`, `FT Group`), allowing for detailed analysis of how metrics vary across different segments.
- **Vega-Lite Config Files:** JSON configuration files for visualizing the statistical metrics.

### Notes
Use `dashboard1-ValRmse.yaml` as a template for the YAML configuration file. The configuration allows display adjustments in Simwrapper.

## Map Visualization (`map.py`)

### Description
Creates geographic visualizations of road data.

### Configuration
Refer to `map_config.ini` for additional map visualization settings.

### Outputs
- **CSV and SHP Files:** Outputs both CSV and SHP formats.
- **Hover Information:** Add hover-over details manually by calling `data.colname` in the scripts.

## Control Script (`validation_road.py`)

### Description
Orchestrates the entire validation process by calling the necessary scripts and passing appropriate configurations.

### Configuration
Controlled through the `config.toml` file, which includes:
- Paths to loaded network files and observed data.
- Column mappings and extra columns to be used.
- Settings for scatter plots, statistical metrics, and map visualizations.

---

### Notes:
- **Variable Naming Convention:** For consistency, use `lowercase_with_underscores` for all variable and file names in Python scripts. This aligns with PEP 8 guidelines and avoids mixing conventions.
- **CSV File Naming:** The file naming convention has been adjusted to use `lowercase_with_underscores`, so the output CSV is now `{chosen_time_period}_Scatter_data.csv`.


# Transit Model Validation Scripts

## Setup Instructions

Before running the scripts, configure the paths to all necessary input and output files in the control file as described below.

### Input Configuration

Specify the paths for the required input files under the `[transit]` section in the control file:

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
