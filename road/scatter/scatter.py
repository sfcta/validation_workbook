import os
import pandas as pd
from simpledbf import Dbf5
import configparser
import json
import yaml
from sklearn.linear_model import LinearRegression

def handle_merged_header(excel_file, sheet_name, extra_columns):
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, skiprows=1)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    return df[extra_columns]

def filter_and_aggregate(excel_file, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns):
    excel_df = handle_merged_header(excel_file, sheet_name, extra_columns)
    base_df = excel_df.reset_index(drop=True)

    for col in output_columns + ['Daily']:
        base_df[col] = 0

    for dbf_file, output_column in zip(dbf_files, output_columns):
        dbf_file_path = f"{dbf_directory}\{dbf_file}"
        dbf = Dbf5(dbf_file_path)
        dbf_df = dbf.to_dataframe()
        dbf_df = dbf_df[column_names]

        dbf_dict = {(row['A'], row['B']): row for index, row in dbf_df.iterrows()}

        for index, row in base_df.iterrows():
            key = (row['A'], row['B'])
            if key in dbf_dict:
                dbf_row = dbf_dict[key]

                for extra_col in column_names:
                    if extra_col not in ['A', 'B', 'V_1']:
                        base_df.at[index, extra_col] = dbf_row[extra_col]

                base_df.at[index, output_column] = dbf_row['V_1']

                base_df.at[index, 'Daily'] += dbf_row['V_1']

    return base_df
os.chdir(os.path.dirname(os.path.abspath(__file__)))


config = configparser.ConfigParser()
config.read('config_template.ini')

dbf_directory = config['DEFAULT']['DBF_Directory']
dbf_files = [dbf_file.strip() for dbf_file in config['DEFAULT']['DBF_Files'].split(',')]
excel_file_path = config['DEFAULT']['Excel_File_Path']
sheet_name = config['DEFAULT']['Sheet_Name']
column_names = [name.strip() for name in config['DEFAULT']['DBF_Column_Names'].split(',')]
output_columns = ['AM', 'MD', 'PM', 'EV', 'EA']
extra_columns = [col.strip() for col in config['DEFAULT']['Excel_Extra_Columns'].split(',')]
obs_usecols = config['DEFAULT']['Obs_usecols']
chosen_timeperoid = config['DEFAULT']['Chosen_period']
classification_col = config['DEFAULT']['Classification_col']
# selected_cols = [col.strip() for col in config['DEFAULT']['Selected_cols'].split(',')]
combined_df_cols = [col.strip() for col in config['DEFAULT']['Combined_DF_Cols'].split(',')]

fields1 = config['PLOT1']['Fields'].split(', ')
nominal_fields1 = config['PLOT1']['NominalFields'].split(', ')
x_field1 = config['PLOT1']['XField']
y_field1 = config['PLOT1']['YField']
name1 = config['PLOT1']['Name']

fields2 = config['PLOT2']['Fields'].split(', ')
nominal_fields2 = config['PLOT2']['NominalFields'].split(', ')
x_field2 = config['PLOT2']['XField']
y_field2 = config['PLOT2']['YField']
name2 = config['PLOT2']['Name']

dashboard_num = config['YAML']['Dashboard_number']

if len(dbf_files) != len(output_columns):
    raise ValueError("The number of DBF files and output column names do not match.")

est_df = filter_and_aggregate(excel_file_path, sheet_name, dbf_directory, dbf_files, column_names, output_columns, extra_columns)

obs_df = pd.read_excel(excel_file_path, skiprows=1, sheet_name=sheet_name, usecols=obs_usecols)

common_columns = obs_df.columns

error = {}
for col in common_columns:
    error[col] = est_df[col] - obs_df[col]
error_df = pd.DataFrame(error)
error_squared_df = error_df.pow(2)
error_percent_df = error_df.divide(obs_df).fillna(0)
error_percent_df = error_percent_df.apply(lambda x: x * 100, axis=1)

combined_df = pd.concat([est_df, obs_df, error_df, error_squared_df, error_percent_df], axis=1)
Select_time_period_df = combined_df.filter(like=chosen_timeperoid)
specific_columns_combined_df = combined_df[combined_df_cols]
Select_time_period_loc_df = pd.concat([specific_columns_combined_df, Select_time_period_df], axis=1)
calculation_cols = ['Estimated Volume', 'Observed Volume', 'Errors','Squared Errors', 'Percent Errors']
Select_time_period_loc_df.columns = combined_df_cols + calculation_cols
Select_time_period_loc_df = Select_time_period_loc_df.drop_duplicates(subset=['A', 'B'], keep='first')

file_name = f"{chosen_timeperoid}_all_data.csv"
csv_base_name = os.path.splitext(file_name)[0]
Select_time_period_loc_df.to_csv(file_name, index=False)
classification_col_types = Select_time_period_loc_df[classification_col].unique()

def sanitize_filename(filename):
    return filename.replace('/', '_').replace('\\', '_')

def generate_vega_lite_json_EST(csv_file, classification_col, classification_col_types, x_field, y_field, fields, nominal_fields, name, include_all_data = False):
    df = pd.read_csv(csv_file)
    if not include_all_data:
        df = df[df[classification_col] == classification_col_types]
    max_value = max(df[x_field].max(), df[y_field].max())
    model = LinearRegression().fit(df[[x_field]], df[y_field])
    m = model.coef_[0]
    b = model.intercept_

    regression_df = pd.DataFrame({
        x_field: [df[x_field].min(), df[x_field].max()],
        y_field: [m * df[x_field].min() + b, m * df[x_field].max() + b]
    })
    diagonal_df = pd.DataFrame({
        x_field: [df[x_field].min(), df[x_field].max()],
        y_field: [df[x_field].min(), df[x_field].max()]  # Diagonal line: y = x
    }) 
    equation_text = f"y = {m:.2f}x + {b:.2f}"
    max_value = max(df[x_field].max(), df[y_field].max())
    tooltip_config = [{"field": field, "type": "nominal" if field in nominal_fields else "quantitative", "title": field} for field in fields]
    
    vega_lite_config = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A scatterplot with a regression line",
        "data": {"url": csv_file},
        "transform": [] if include_all_data else [{"filter": f"datum['{classification_col}'] == '{classification_col_types}'"}],
        "layer": [
            {
                "mark": {
                    "type": "rule",
                    "color": "grey",
                    "strokeWidth": 3
                },
                "encoding": {
                    "y": {"datum": 0}
                }
            },
            {
                "mark": "point",
                "encoding": {
                    "x": {"field": x_field, "type": "quantitative", "scale": {"domain": [0, max_value]}},
                    "y": {"field": y_field, "type": "quantitative", "scale": {"domain": [0, max_value]}},
                    "tooltip": tooltip_config
                }
            },
            {
                "mark": {
                    "type": "text",
                    "align": "left",
                    "baseline": "bottom",
                    "dx": 5, 
                    "dy": -5
                },
                "data": {"values": [{"text": equation_text}]},
                "encoding": {
                    "x": {"value": 0},
                    "y": {"value": 0},
                    "text": {"field": "text", "type": "nominal"},
                    "color": {"value": "red"}
                }
            },
            {
                "mark": "line",
                "data": {"values": diagonal_df.to_dict(orient='records')},
                "encoding": {
                    "x": {"field": x_field, "type": "quantitative"},
                    "y": {"field": y_field, "type": "quantitative"},
                    "color": {"value": "blue"}
                }
            },
            {
                "mark": "line",
                "data": {"values": regression_df.to_dict(orient='records')},
                "encoding": {
                    "x": {"field": x_field, "type": "quantitative"},
                    "y": {"field": y_field, "type": "quantitative"},
                    "color": {"value": "red"}
                }
            },
        ]
    }
    
    base_name = os.path.splitext(csv_file)[0]
    file_path = f"{sanitize_filename(classification_col_types)}_{name}.vega.json"
    with open(file_path, 'w') as file:
        json.dump(vega_lite_config, file, indent=4)

def generate_vega_lite_json_Diffpercent(csv_file, classification_col_types, x_field, y_field, fields, nominal_fields, name, include_all_data = False):
    
    tooltip_config = [{"field": field, "type": "nominal" if field in nominal_fields else "quantitative", "title": field} for field in fields]
    vega_lite_config = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A scatterplot",
        "data": {"url": csv_file},
        "transform": [] if include_all_data else [{"filter": f"datum['{classification_col}'] == '{classification_col_types}'"}],
         "layer": [
                {
            "mark": {
                "type": "rule",
                "color": "grey",
                "strokeWidth": 3
            },
            "encoding": {
                "y": {"datum": 0}
            }
        },    
        {
            "mark": "point",
            "encoding": {
                "x": {"field": x_field, "type": "quantitative"},
                "y": {"field": y_field, "type": "quantitative"},
                "tooltip": tooltip_config
            }
        }
    ]
}

    base_name = os.path.splitext(csv_file)[0]
    file_path = f"{sanitize_filename(classification_col_types)}_{name}.vega.json"
    with open(file_path, 'w') as file:
        json.dump(vega_lite_config, file, indent=4)

def generate_yaml_config(classification_col_type, name1, name2, include_all_data=False):
    suffix = 'all' if include_all_data else sanitize_filename(classification_col_type)
    
    return [
        {
            "title": f"{suffix} - {name1}",
            "type": "vega",
            "description": "",
            "config": f"{suffix}_{name1}.vega.json"
        },
        {
            "title": f"{suffix} - {name2}",
            "type": "vega",
            "description": "",
            "config": f"{suffix}_{name2}.vega.json"
        }
    ]

yaml_config = {
    "header": {
        "tab": "Scatter plots",
        "title": "Scatter plots",
        "description": ""
    },
    "layout": {}
}


row_count = 1
for types in classification_col_types:
    generate_vega_lite_json_EST(file_name, classification_col,types, x_field1, y_field1, fields1, nominal_fields1,name1)
    generate_vega_lite_json_Diffpercent(file_name, types, x_field2, y_field2, fields2, nominal_fields2,name2)

    yaml_config['layout'][f'row{row_count}'] = generate_yaml_config(types, name1, name2)
    row_count += 1

generate_vega_lite_json_EST(file_name,classification_col, 'all', x_field1, y_field1, fields1, nominal_fields1, name1, include_all_data=True)
generate_vega_lite_json_Diffpercent(file_name, 'all', x_field2, y_field2, fields2, nominal_fields2, name2, include_all_data=True)

yaml_config['layout'][f'row{row_count}'] = generate_yaml_config('all', name1, name2, include_all_data=True)
yaml_file_path = f"dashboard{dashboard_num}-Scatter.yaml"
with open(yaml_file_path, 'w') as file:
    yaml.dump(yaml_config, file, default_flow_style=False)