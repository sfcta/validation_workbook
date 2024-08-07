# Road Validation Documentation

This repository contains scripts and documentation for the validation of road models using the Simwrapper environment. Below you'll find details about each component, including the expected input, output, and configuration files.

## Contents

- [Overview](#overview)
- [Usage](#usage)
- [Scatter Plot Validation](#scatter-plot-validation)
- [Statistical Metrics Calculation](#statistical-metrics-calculation)
- [Map Visualization](#map-visualization)

## Overview

The validation process is controlled by a main script, `validation_road.py`, which orchestrates the execution of specific functionality scripts and uses a TOML configuration file for parameterization.

## Usage

1. **Configuration**: Ensure the `config.toml` file is properly set up with the correct paths and parameters for your data and validation process.
2. **Execution**: Run the main validation script `validation_road.py` to perform the entire validation process. You can run the script using the provided batch file `validation.bat` or directly through a Python interpreter.

## Scatter Plot Validation

### Description
Generates scatter plots for road validation using time-specific dbf files.

### Scripts
Located at `scatter.py`

### Inputs
- **DBF Files:** Requires 5 dbf files for different time periods (`AM`, `MD`, `PM`, `EV`, `EA`).
- **Template Structure:** Observations should follow the structure outlined in the `Excel_File_Path` specified in the `config.toml` file (Loc Data tab).

### Outputs
- **JSON Files:** Two Vega-Lite configuration files for each grouping variable.
- **CSV:** A detailed CSV file named `{Chosen_time_period}_Scatter_data.csv` with all desired columns.

### Configuration
Edit specific titles and descriptions in the generated YAML file.

## Statistical Metrics Calculation

### Description
Calculates and exports several statistical metrics for road validation.

### Scripts
Located at `stats.py`

### Outputs
- **CSV Files:** Three concise CSVs (`percent_rmse`, `relative_error`, `est_obs_ratio`) and their melted versions, totaling 7 files.
- **Vega-Lite Config Files:** JSON configuration files for visualizing the statistical metrics.

### Notes
Use `dashboard1-ValRmse.yaml` as a template for the YAML configuration file. The configuration allows display adjustments in Simwrapper.

## Map Visualization

### Description
Creates geographic visualizations of road data.

### Scripts
Located at `map.py`

### Configuration
Refer to `map_config.ini` for additional map visualization settings.

### Outputs
- **CSV and SHP Files:** Outputs both CSV and SHP formats.
- **Hover Information:** Add hover-over details manually by calling `data.colname` in the scripts.

## Control Script

### Description
Orchestrates the entire validation process by calling the necessary scripts and passing appropriate configurations.

### Script
Located at `validation_road.py`

### Configuration
Controlled through the `config.toml` file, which includes:
- Paths to DBF files and observed data.
- Column mappings and extra columns to be used.
- Settings for scatter plots, statistical metrics, and map visualizations.