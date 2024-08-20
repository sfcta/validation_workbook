import os
import json
import yaml
import pandas as pd
from sklearn.linear_model import LinearRegression


def compute_and_save_errors(
        est_df,
        obs_df,
        chosen_timeperiod,
        combined_df_cols,
        classification_col,
        file_name):
    # Compute errors
    times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']
    error = {col: est_df[col] - obs_df[col] for col in times}
    error_df = pd.DataFrame(error)
    error_squared_df = error_df.pow(2)
    error_percent_df = error_df.divide(obs_df[times]).fillna(0)
    error_percent_df = error_percent_df.apply(lambda x: x * 100, axis=1)

    # Combine dataframes
    combined_df = pd.concat(
        [est_df, obs_df[times], error_df, error_squared_df, error_percent_df], axis=1)
    select_time_period_df = combined_df.filter(like=chosen_timeperiod)
    specific_columns_combined_df = combined_df[combined_df_cols]
    select_time_period_loc_df = pd.concat(
        [specific_columns_combined_df, select_time_period_df], axis=1)

    # Set column names
    calculation_cols = [
        'Estimated Volume',
        'Observed Volume',
        'Errors',
        'Squared Errors',
        'Percent Errors']
    select_time_period_loc_df.columns = combined_df_cols + calculation_cols

    # Drop duplicates
    select_time_period_loc_df = select_time_period_loc_df.drop_duplicates(
        subset=['A', 'B'], keep='first')

    # Save to CSV
    select_time_period_loc_df.to_csv(file_name, index=False)

    # Get classification column types
    classification_col_types = select_time_period_loc_df[classification_col].unique(
    )

    return select_time_period_loc_df, classification_col_types, file_name


def sanitize_filename(filename):
    return filename.replace('/', '_').replace('\\', '_')


def generate_vega_lite_json_est(
        csv_file,
        classification_col,
        classification_col_types,
        x_field,
        y_field,
        fields,
        nominal_fields,
        name,
        include_all_data=False):
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
    tooltip_config = [{"field": field,
                       "type": "nominal" if field in nominal_fields else "quantitative",
                       "title": field} for field in fields]

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


def generate_vega_lite_json_diffpercent(
        csv_file,
        classification_col,
        classification_col_types,
        x_field,
        y_field,
        fields,
        nominal_fields,
        name,
        include_all_data=False):

    tooltip_config = [{"field": field,
                       "type": "nominal" if field in nominal_fields else "quantitative",
                       "title": field} for field in fields]
    vega_lite_config = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A scatterplot",
        "data": {
            "url": csv_file},
        "transform": [] if include_all_data else [
            {
                "filter": f"datum['{classification_col}'] == '{classification_col_types}'"}],
        "layer": [
            {
                "mark": {
                    "type": "rule",
                            "color": "grey",
                            "strokeWidth": 3},
                "encoding": {
                    "y": {
                        "datum": 0}}},
            {
                "mark": "point",
                "encoding": {
                    "x": {
                        "field": x_field,
                        "type": "quantitative"},
                    "y": {
                        "field": y_field,
                        "type": "quantitative"},
                    "tooltip": tooltip_config}}]}

    base_name = os.path.splitext(csv_file)[0]
    file_path = f"{sanitize_filename(classification_col_types)}_{name}.vega.json"
    with open(file_path, 'w') as file:
        json.dump(vega_lite_config, file, indent=4)


def generate_yaml_config(
        classification_col_type,
        name1,
        name2,
        include_all_data=False):
    suffix = 'all' if include_all_data else sanitize_filename(
        classification_col_type)

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
