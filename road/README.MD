# Road Validation Documentation

This repository contains scripts and documentation for the validation of road models using the Simwrapper environment. Below you'll find details about each component, including the expected input, output, and configuration files.

## Contents

- [Scatter Plot Validation](#scatter-plot-validation)
- [Statistical Metrics Calculation](#statistical-metrics-calculation)
- [Map Visualization](#map-visualization)

## Scatter Plot Validation

### Description
Generates scatter plots for road validation using time-specific dbf files.

### Scripts
Located at `Q:\Model Development\Simwrapper Validation\notebooks\roads\Scatter\scatter.py`

### Inputs
- **DBF Files:** Requires 5 dbf files for different time periods (`AM`, `MD`, `PM`, `EV`, `EA`).
- **Template Structure:** Observations should follow the structure outlined in `Q:\Model Development\Simwrapper Validation\templates\Road_Validation_Draft_v2_Run17.xlsx` (Loc Data tab).

### Outputs
- **JSON Files:** Two Vega-Lite configuration files for each grouping variable.
- **CSV:** A detailed CSV file named `{Chosen_time_period}_all_data.csv` with all desired columns.

### Configuration
Edit specific titles and descriptions in the generated YAML file.

## Statistical Metrics Calculation

### Description
Calculates and exports several statistical metrics for road validation.

### Scripts
Located at `Q:\Model Development\Simwrapper Validation\notebooks\roads\RMSE\calculation.py`

### Configuration
Adjust the `Group_var` in `Q:\Model Development\Simwrapper Validation\notebooks\roads\RMSE\config_template.ini`.

### Outputs
- **CSV Files:** Three concise CSVs (`rmse`, `relative error`, `E/O`) and their melted versions, totaling 7 files.

### Notes
Use `dashboard1-ValRmse.yaml` as a template for the YAML configuration file. The configuration allows display adjustments in Simwrapper.

## Map Visualization

### Description
Creates geographic visualizations of road data.

### Scripts
Located at `X:\Projects\Miscellaneous\validation_simwrapper\roads\map\shapefile.py`

### Configuration
Refer to `X:\Projects\Miscellaneous\validation_simwrapper\roads\map\map_config.ini`.

### Outputs
- **CSV and SHP Files:** Outputs both CSV and SHP formats.
- **Hover Information:** Add hover-over details manually by calling `data.colname` in the scripts.

