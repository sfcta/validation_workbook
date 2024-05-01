# 
[folder_setting]

transit_input_dir                = Q:\Model Development\Simwrapper Validation\input\Transit
transit_output_dir               = Q:\Model Development\Simwrapper Validation\notebooks\transit\Output
control_file                     = Q:\\Model Development\\Simwrapper Validation\\notebooks\\transit\\script\\transit.ctl
markdown_output_dir              = X:\Projects\Miscellaneous\validation_simwrapper\transit\table
MUNI_output_dir                  = X:\Projects\Miscellaneous\validation_simwrapper\transit\val_MUNI
BART_output_dir                  = X:\Projects\Miscellaneous\validation_simwrapper\transit\val_BART
Screenline_output_dir            = X:\Projects\Miscellaneous\validation_simwrapper\transit\val_Screenline
total_output_dir                 = X:\Projects\Miscellaneous\validation_simwrapper\transit\val_Total
Base_model_dir                   = X:\Projects\DTX\CaltrainValidation\s8_2019_Base
SHP_file_dir                     = X:\Projects\Miscellaneous\validation_simwrapper\transit\shp
necessary_scripts_for_vmt_folder = Y:/Users/Patricie/champ/champ/scripts/summarize

# Input 

[transit]

SFALLMSAAM_DBF                   = SFALLMSAAM.DBF
SFALLMSAPM_DBF                   = SFALLMSAPM.DBF
SFALLMSAMD_DBF                   = SFALLMSAMD.DBF
SFALLMSAEV_DBF                   = SFALLMSAEV.DBF
SFALLMSAEA_DBF                   = SFALLMSAEA.DBF
Nodes_File_Name                  = nodes.xls
Line_Name_File                   = transitLineToVehicle.csv
Line_Rename_File                 = transit_line_rename.csv
Transit_Templet                  = Transit_Validation_2019 - MA.xlsx
FREEFLOW_SHP                     = freeflow.shp

# Output

[output]

model_BART                       = model_BART.csv
model_BART_county                = model_BART_county.csv
model_BART_SL                    = model_BART_SL.csv
model_MUNI_Line                  = model_MUNI_Line.csv
model_SL                         = model_SL.csv




