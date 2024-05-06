Validation/road Documentation

Scatter Plot.
The script is in here Q:\Model Development\Simwrapper Validation\notebooks\roads\Scatter\scatter.py
There must be 5 dbf files ( ['AM', 'MD', 'PM', 'EV', 'EA']) as the input. And the observations follow the structure of  "Q:\Model Development\Simwrapper Validation\templates\Road_Validation_Draft_v2_Run17.xlsx" tab (Loc Data), because the first line was skipped
The output should have 2 scatter vaga.json corresponding to each element in the grouping variable. And a csv file containing every cols we want named by {Chosen_time_period_ for calculating_all_metrics}_all_data.csv
The specific titlies and description need to be edited in the generated yaml file
Stats
Script is in Q:\Model Development\Simwrapper Validation\notebooks\roads\RMSE\calculation.py
Eveytime changing the Group_var in Q:\Model Development\Simwrapper Validation\notebooks\roads\RMSE\config_template.ini, the script will generate 3 concise csv(rmse, relative err, E/O)and their melted version, except the “xxx_count.csv”. (so 7 in total). This is because Simwrapper does not support the group bar chart in vegalite documentation. In order to display this kind of chart, the imported csv needs to be melted, which means 1 record per row.
Since each row in Simwrapper contains the data for a single metrics but including all grouping methods, the yaml file needs to be written after all csv has been generated. But you can use the one “dashboard1-ValRmse.yaml” as a template.
Map
Script is in X:\Projects\Miscellaneous\validation_simwrapper\roads\map\shapefile.py
The config file is here X:\Projects\Miscellaneous\validation_simwrapper\roads\map\map_config.ini
The parameters we might want to change are in YAML section. And the hover-over stuff is better for us to add manually. * the output data have 2 versions, 1 is a csv, and 1 is a shp. When we want to add the hover things, we need to call data.colname
