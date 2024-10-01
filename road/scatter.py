import json
import pandas as pd
from pathlib import Path
from sklearn.linear_model import LinearRegression
from validation_road_utils import compute_and_combine_scatter
def compute_and_save_errors(
        est_df,
        obs_df,
        chosen_timeperiod,
        combined_df_cols,
        classification_col,
        file_name):
    # Compute errors
    times = ['Daily', 'AM', 'MD', 'PM', 'EV', 'EA']

    # Combine dataframes using the utility function
    select_time_period_loc_df = compute_and_combine_scatter(est_df, obs_df, times, combined_df_cols, chosen_timeperiod)
    
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
    classification_col_types = select_time_period_loc_df[classification_col].unique()

    return select_time_period_loc_df, classification_col_types, file_name

def generate_vega_lite_json_est(
        obs_file,
        classification_col,
        classification_col_types,
        x_field,
        y_field,
        fields,
        nominal_fields,
        name,
        output_template,
        include_all_data=False):
    df = pd.read_csv(obs_file)

    # Adjust the filtering for single value (string) vs list
    if not include_all_data:
        if isinstance(classification_col_types, str):
            df = df[df[classification_col] == classification_col_types]  # Single value filter
        else:
            df = df[df[classification_col].isin(classification_col_types)]  # List-like filter

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
    tooltip_config = [{"field": field,
                       "type": "nominal" if field in nominal_fields else "quantitative",
                       "title": field} for field in fields]

    vega_lite_config = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A scatterplot with a regression line",
        "data": {"url": obs_file},
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

    file_path = Path(output_template.format(classification_col_types=classification_col_types, name=name))
    with open(file_path, 'w') as file:
        json.dump(vega_lite_config, file, indent=4)


def generate_vega_lite_json_diffpercent(
        obs_file,
        classification_col,
        classification_col_types,
        x_field,
        y_field,
        fields,
        nominal_fields,
        name,
        output_template,
        include_all_data=False):

    tooltip_config = [{"field": field,
                       "type": "nominal" if field in nominal_fields else "quantitative",
                       "title": field} for field in fields]
    vega_lite_config = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A scatterplot",
        "data": {
            "url": obs_file},
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

    file_path = Path(output_template.format(classification_col_types=classification_col_types, name=name))
    with open(file_path, 'w') as file:
        json.dump(vega_lite_config, file, indent=4)