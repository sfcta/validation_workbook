from pathlib import Path

import pandas as pd
import tomllib
from utils import dataframe_to_markdown, format_dataframe


def convert_to_integer(value):
    try:
        return int(value)
    except ValueError:
        return value


def process_data(
    obs_MUNI_line,
    model_MUNI_line,
    filters,
    groupby_column,
    sum_column,
    rename_column,
    join_method,
):
    """
    Process MUNI data with flexible filtering, grouping, and summing.

    Parameters:
    obs_MUNI_line (DataFrame): The observed MUNI line data.
    model_MUNI_line (DataFrame): The modeled MUNI line data.
    filters (list of tuples): List of filter conditions, where each tuple contains (column_name, value).
    groupby_column (str): Column name to group by.
    sum_column (str): Column name to sum.
    rename_column (str): New name to rename the groupby column to in the output.

    Returns:
    DataFrame: The processed and formatted DataFrame.
    """

    # Apply the custom function to convert values
    model_MUNI_line[groupby_column] = model_MUNI_line[groupby_column].apply(
        convert_to_integer
    )
    obs_MUNI_line[groupby_column] = obs_MUNI_line[groupby_column].apply(
        convert_to_integer
    )

    # Apply filters
    if filters is not None:
        for filter_col, filter_val in filters:
            obs_MUNI_line = obs_MUNI_line[obs_MUNI_line[filter_col] == filter_val]
            model_MUNI_line = model_MUNI_line[model_MUNI_line[filter_col] == filter_val]

    # Processing observed data
    MUNI_IB_obs = obs_MUNI_line.groupby(groupby_column)[sum_column].sum().reset_index()
    MUNI_IB_obs.rename(
        columns={groupby_column: rename_column, sum_column: "Observed"}, inplace=True
    )

    # Processing modeled data
    MUNI_IB_model = (
        model_MUNI_line.groupby(groupby_column)[sum_column].sum().reset_index()
    )
    MUNI_IB_model.rename(
        columns={groupby_column: rename_column, sum_column: "Modeled"}, inplace=True
    )

    # Merging observed and modeled data
    MUNI_IB = pd.merge(MUNI_IB_obs, MUNI_IB_model, on=rename_column, how=join_method)

    # Calculating total row and appending it
    total_row = pd.Series(MUNI_IB[["Observed", "Modeled"]].sum(), name="Total")
    MUNI_IB = pd.concat([MUNI_IB, total_row.to_frame().T], ignore_index=True)

    # Calculating 'Diff' and 'Percentage Diff'
    MUNI_IB["Diff"] = MUNI_IB["Modeled"] - MUNI_IB["Observed"]
    MUNI_IB["Percentage Diff"] = MUNI_IB["Diff"] * 100 / MUNI_IB["Observed"]

    # Assign 'Total' label to the last row in the renamed column
    MUNI_IB.at[MUNI_IB.index[-1], rename_column] = "Total"

    # Formatting the DataFrame
    numeric_cols = ["Observed", "Modeled", "Diff"]
    MUNI_IB = format_dataframe(
        MUNI_IB, numeric_columns=numeric_cols, percentage_columns=["Percentage Diff"]
    )

    return MUNI_IB


# Valdiation for BART
def append_group_total(df, stations, group_name):
    # Filter the DataFrame for the specified stations
    df_group = df[df["Station"].isin(stations)]

    # Calculate the sum for the filtered rows
    group_total = pd.Series(df_group[["Observed", "Modeled"]].sum(), name=group_name)

    # Append the total row to the DataFrame with the group name
    df = pd.concat([df, group_total.to_frame().T], ignore_index=True)
    df.at[df.index[-1], "Station"] = group_name
    return df


def process_bart_data(
    obs_bart_line,
    model_bart_line,
    direction_filter,
    tod_filter,
    groupby_column,
    sum_column,
):
    """
    Process BART data from Excel and CSV files with flexible grouping and summing.

    Parameters:
    obs_bart_line (str): Path to the obs_bart CSV file.
    model_bart_line (str): Path to the model_bart CSV file.
    direction_filter (str): Direction to filter the data by.
    tod_filter (str): Time of Day
    groupby_column (str): Column name to group by.
    sum_column (str): Column name to sum.

    Returns:
    pd.DataFrame: The processed and formatted DataFrame.
    """

    # Apply the custom function to convert values
    model_bart_line[groupby_column] = model_bart_line[groupby_column].apply(
        convert_to_integer
    )

    # Determine filter conditions
    obs_condition = pd.Series([True] * len(obs_bart_line))
    model_condition = pd.Series([True] * len(model_bart_line))

    if direction_filter is not None:
        obs_condition &= obs_bart_line["Direction"] == direction_filter
        model_condition &= model_bart_line["Direction"] == direction_filter

    if tod_filter is not None:
        obs_condition &= obs_bart_line["TOD"] == tod_filter
        model_condition &= model_bart_line["TOD"] == tod_filter

    # Processing observed data
    MUNI_IB_obs = (
        obs_bart_line[obs_condition]
        .groupby(groupby_column)[sum_column]
        .sum()
        .reset_index()
    )
    MUNI_IB_obs.rename(
        columns={groupby_column: "Station", sum_column: "Observed"}, inplace=True
    )

    # Processing modeled data
    MUNI_IB_model = (
        model_bart_line[model_condition]
        .groupby(groupby_column)[sum_column]
        .sum()
        .reset_index()
    )
    MUNI_IB_model.rename(
        columns={groupby_column: "Station", sum_column: "Modeled"}, inplace=True
    )

    # Merging observed and modeled data
    MUNI_IB = pd.merge(MUNI_IB_obs, MUNI_IB_model, on="Station", how="left")
    all = [
        "EMBR",
        "MONT",
        "POWL",
        "CIVC",
        "16TH",
        "24TH",
        "GLEN",
        "BALB",
        "12TH",
        "19TH",
        "DALY",
    ]
    MUNI_IB = MUNI_IB[MUNI_IB["Station"].isin(all)]

    # Calculating total row and appending it
    # Define your station groups
    station_core_SF = ["EMBR", "MONT", "POWL", "CIVC"]
    station_outer_SF = ["16TH", "24TH", "GLEN", "BALB"]
    station_oakland = ["12TH", "19TH"]

    # Use the function to append totals for each group
    MUNI_IB = append_group_total(MUNI_IB, station_core_SF, "Core SF")
    MUNI_IB = append_group_total(MUNI_IB, station_outer_SF, "Outer SF")
    MUNI_IB = append_group_total(MUNI_IB, station_oakland, "Oakland Core")
    MUNI_IB = append_group_total(MUNI_IB, all, "All Listed Stations")

    # Calculating 'Diff' and 'Percentage Diff'
    MUNI_IB["Diff"] = MUNI_IB["Modeled"] - MUNI_IB["Observed"]
    MUNI_IB["Percentage Diff"] = MUNI_IB["Diff"] * 100 / MUNI_IB["Observed"]

    # Formatting the DataFrame
    numeric_cols = ["Observed", "Modeled", "Diff"]
    MUNI_IB = format_dataframe(
        MUNI_IB,
        numeric_columns=numeric_cols,
        percentage_columns=["Percentage Diff"],
    )

    return MUNI_IB


def sort_dataframe_by_custom_order(df, order_column, custom_order):
    """
    Sort the DataFrame rows according to a custom order specified for one column.

    Parameters:
    df (pd.DataFrame): The DataFrame to sort.
    order_column (str): The name of the column to apply the custom order.
    custom_order (list): The list specifying the custom order of values.

    Returns:
    pd.DataFrame: The sorted DataFrame.
    """
    # Create a categorical data type with the custom order
    category_order = pd.CategoricalDtype(categories=custom_order, ordered=True)

    # Convert the order column to this categorical type
    df[order_column] = pd.Categorical(
        df[order_column], categories=custom_order, ordered=True
    )

    # Sort the DataFrame by the order column
    df = df.sort_values(by=order_column)

    # Reset index after sorting
    df = df.reset_index(drop=True)

    return df


def process_mkd_muni(
    transit_input_dir,
    observed_MUNI_Line,
    output_transit_dir,
    model_MUNI_Line,
    markdown_output_dir,
    MUNI_output_dir,
    MUNI_ib_day,
    MUNI_ob_day,
    MUNI_ib_am,
    MUNI_ib_pm,
    MUNI_ob_am,
    MUNI_ob_pm,
    MUNI_mode_day,
    MUNI_mode,
    MUNI_mode_am_md,
    MUNI_mode_am,
    MUNI_mode_pm_md,
    MUNI_mode_pm,
    MUNI_tod_md,
    MUNI_tod,
    MUNI_EB_md,
    MUNI_EB,
    MUNI_LB_md,
    MUNI_LB,
    MUNI_Rail_md,
    MUNI_Rail,
    MUNI_IB,
    MUNI_OB,
):
    obs_MUNI_line_df = pd.read_csv(transit_input_dir / observed_MUNI_Line)
    model_MUNI_line_df = pd.read_csv(output_transit_dir / model_MUNI_Line)
    tod_order = ["EA", "AM", "MD", "PM", "EV", "Total"]
    MUNI_IB_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "IB")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    dataframe_to_markdown(
        MUNI_IB_df,
        file_name=Path(markdown_output_dir / MUNI_ib_day),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_OB_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "OB")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    MUNI_OB_df = pd.merge(MUNI_IB_df[["Route"]], MUNI_OB_df, on="Route", how="left")
    dataframe_to_markdown(
        MUNI_OB_df,
        file_name=Path(markdown_output_dir / MUNI_ob_day),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_IB_df.to_csv(MUNI_output_dir / MUNI_IB, index=False)
    MUNI_OB_df.to_csv(MUNI_output_dir / MUNI_OB, index=False)
    MUNI_IB_AM_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "IB"), ("TOD", "AM")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    MUNI_IB_AM_df = pd.merge(
        MUNI_IB_df[["Route"]], MUNI_IB_AM_df, on="Route", how="left"
    )
    MUNI_IB_AM_df = MUNI_IB_AM_df.fillna("-")
    dataframe_to_markdown(
        MUNI_IB_AM_df,
        file_name=Path(markdown_output_dir / MUNI_ib_am),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_IB_PM_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "IB"), ("TOD", "PM")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    MUNI_IB_PM_df = pd.merge(
        MUNI_IB_df[["Route"]], MUNI_IB_PM_df, on="Route", how="left"
    )
    MUNI_IB_PM_df = MUNI_IB_PM_df.fillna("-")
    dataframe_to_markdown(
        MUNI_IB_PM_df,
        file_name=Path(markdown_output_dir / MUNI_ib_pm),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_OB_AM_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "OB"), ("TOD", "AM")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    MUNI_OB_AM_df = pd.merge(
        MUNI_IB_df[["Route"]], MUNI_OB_AM_df, on="Route", how="left"
    )
    MUNI_OB_AM_df = MUNI_OB_AM_df.fillna("-")
    dataframe_to_markdown(
        MUNI_OB_AM_df,
        file_name=Path(markdown_output_dir / MUNI_ob_am),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_OB_PM_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Direction", "OB"), ("TOD", "PM")],
        "Line",
        "Ridership",
        "Route",
        "left",
    )
    MUNI_OB_PM_df = pd.merge(
        MUNI_IB_df[["Route"]], MUNI_OB_PM_df, on="Route", how="left"
    )
    MUNI_OB_PM_df = MUNI_OB_PM_df.fillna("-")
    dataframe_to_markdown(
        MUNI_OB_PM_df,
        file_name=Path(markdown_output_dir / MUNI_ob_pm),
        highlight_rows=[72],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_mode_df = process_data(
        obs_MUNI_line_df, model_MUNI_line_df, None, "Mode", "Ridership", "Mode", "left"
    )
    dataframe_to_markdown(
        MUNI_mode_df,
        file_name=Path(markdown_output_dir / MUNI_mode_day),
        highlight_rows=[3],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_mode_df[~MUNI_mode_df["Mode"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_mode), index=False
    )
    MUNI_mode_am_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("TOD", "AM")],
        "Mode",
        "Ridership",
        "Mode",
        "left",
    )
    dataframe_to_markdown(
        MUNI_mode_am_df,
        file_name=Path(markdown_output_dir / MUNI_mode_am_md),
        highlight_rows=[3],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_mode_am_df[~MUNI_mode_am_df["Mode"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_mode_am), index=False
    )
    MUNI_mode_pm_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("TOD", "PM")],
        "Mode",
        "Ridership",
        "Mode",
        "left",
    )
    dataframe_to_markdown(
        MUNI_mode_pm_df,
        file_name=Path(markdown_output_dir / MUNI_mode_pm_md),
        highlight_rows=[3],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_mode_pm_df[~MUNI_mode_pm_df["Mode"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_mode_pm), index=False
    )
    MUNI_tod_df = process_data(
        obs_MUNI_line_df, model_MUNI_line_df, None, "TOD", "Ridership", "TOD", "left"
    )
    MUNI_tod_df["TOD"] = pd.Categorical(
        MUNI_tod_df["TOD"], categories=tod_order, ordered=True
    )
    MUNI_tod_df = MUNI_tod_df.sort_values("TOD")
    MUNI_tod_df = MUNI_tod_df.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        MUNI_tod_df,
        file_name=Path(markdown_output_dir / MUNI_tod_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_tod_df[~MUNI_tod_df["TOD"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_tod), index=False
    )
    MUNI_EB_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Mode", "Express Bus")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    MUNI_EB_df["TOD"] = pd.Categorical(
        MUNI_EB_df["TOD"], categories=tod_order, ordered=True
    )
    MUNI_EB_df = MUNI_EB_df.sort_values("TOD")
    MUNI_EB_df = MUNI_EB_df.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        MUNI_EB_df,
        file_name=Path(markdown_output_dir / MUNI_EB_md),
        highlight_rows=[4],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_EB_df[~MUNI_EB_df["TOD"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_EB), index=False
    )
    MUNI_LB_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Mode", "Local Bus")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    MUNI_LB_df["TOD"] = pd.Categorical(
        MUNI_LB_df["TOD"], categories=tod_order, ordered=True
    )
    MUNI_LB_df = MUNI_LB_df.sort_values("TOD")
    MUNI_LB_df = MUNI_LB_df.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        MUNI_LB_df,
        file_name=Path(markdown_output_dir / MUNI_LB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_LB_df[~MUNI_LB_df["TOD"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_LB), index=False
    )
    MUNI_Rail_df = process_data(
        obs_MUNI_line_df,
        model_MUNI_line_df,
        [("Mode", "Rail")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    MUNI_Rail_df["TOD"] = pd.Categorical(
        MUNI_Rail_df["TOD"], categories=tod_order, ordered=True
    )
    MUNI_Rail_df = MUNI_Rail_df.sort_values("TOD")
    MUNI_Rail_df = MUNI_Rail_df.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        MUNI_Rail_df,
        file_name=Path(markdown_output_dir / MUNI_Rail_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    MUNI_Rail_df[~MUNI_Rail_df["TOD"].isin(["Total"])].to_csv(
        Path(MUNI_output_dir / MUNI_Rail), index=False
    )


def process_mkd_bart(
    transit_input_dir,
    observed_BART,
    output_transit_dir,
    model_BART,
    markdown_output_dir,
    BART_output_dir,
    observed_BART_county,
    model_BART_county,
    observed_BART_Screenline,
    Screenline_output_dir,
    tod_order,
    BART_boarding_allday_md,
    BART_boarding_am_md,
    BART_boarding_pm_md,
    BART_at_allday_md,
    BART_at_am_md,
    BART_at_pm_md,
    BART_boarding_allday_csv,
    BART_at_allday_csv,
    county_br_day_csv,
    county_br_am_csv,
    county_br_pm_csv,
    county_at_day_csv,
    county_at_am_csv,
    county_at_pm_csv,
    model_BART_Screenline,
    county_br_day_md,
    county_br_am_md,
    county_br_pm_md,
    county_at_day_md,
    county_at_am_md,
    county_at_pm_md,
    transbay_BART_IB_md,
    transbay_BART_OB_md,
    Countyline_BART_OB_md,
    Countyline_BART_IB_md,
    SF_out_md,
    SF_in_md,
    transbay_BART_IB_csv,
    transbay_BART_OB_csv,
    Countyline_BART_IB_csv,
    Countyline_BART_OB_csv,
    Intra_SF_BART_IB_csv,
    Intra_SF_BART_OB_csv
):
    # Custom order based on the provided list
    custom_order = [
        "19TH",
        "12TH",
        "EMBR",
        "MONT",
        "POWL",
        "CIVC",
        "16TH",
        "24TH",
        "GLEN",
        "BALB",
        "DALY",
        "Core SF",
        "Outer SF",
        "Oakland Core",
        "All Listed Stations",
    ]

    # BART
    obs_BART_line = pd.read_csv(transit_input_dir / observed_BART)
    model_BART_line = pd.read_csv(output_transit_dir / model_BART)
    BART_boarding_allday = process_bart_data(
        obs_BART_line, model_BART_line, None, None, "Station", "Boardings"
    )
    BART_boarding_allday = sort_dataframe_by_custom_order(
        BART_boarding_allday, "Station", custom_order
    )
    dataframe_to_markdown(
        BART_boarding_allday,
        file_name=Path(markdown_output_dir / BART_boarding_allday_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )
    BART_boarding_allday.to_csv(BART_output_dir / BART_boarding_allday_csv, index=False)
    BART_boarding_am = process_bart_data(
        obs_BART_line, model_BART_line, None, "AM", "Station", "Boardings"
    )
    BART_boarding_am = sort_dataframe_by_custom_order(
        BART_boarding_am, "Station", custom_order
    )
    dataframe_to_markdown(
        BART_boarding_am,
        file_name=Path(markdown_output_dir / BART_boarding_am_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )
    BART_boarding_pm = process_bart_data(
        obs_BART_line, model_BART_line, None, "PM", "Station", "Boardings"
    )
    BART_boarding_pm = sort_dataframe_by_custom_order(
        BART_boarding_pm, "Station", custom_order
    )
    dataframe_to_markdown(
        BART_boarding_pm,
        file_name=Path(markdown_output_dir / BART_boarding_pm_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )
    BART_at_allday = process_bart_data(
        obs_BART_line, model_BART_line, None, None, "Station", "Alightings"
    )
    BART_at_allday = sort_dataframe_by_custom_order(
        BART_at_allday, "Station", custom_order
    )
    dataframe_to_markdown(
        BART_at_allday,
        file_name=Path(markdown_output_dir / BART_at_allday_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )
    BART_at_allday.to_csv(BART_output_dir / BART_at_allday_csv, index=False)
    BART_at_am = process_bart_data(
        obs_BART_line, model_BART_line, None, "AM", "Station", "Alightings"
    )
    BART_at_am = sort_dataframe_by_custom_order(BART_at_am, "Station", custom_order)
    dataframe_to_markdown(
        BART_at_am,
        file_name=Path(markdown_output_dir / BART_at_am_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )
    BART_at_pm = process_bart_data(
        obs_BART_line, model_BART_line, None, "PM", "Station", "Alightings"
    )
    BART_at_pm = sort_dataframe_by_custom_order(BART_at_pm, "Station", custom_order)
    dataframe_to_markdown(
        BART_at_pm,
        file_name=Path(markdown_output_dir / BART_at_pm_md),
        highlight_rows=[11, 12, 13, 14],
        center_align_columns=None,
        column_widths=80,
    )

    obs_BART_county = pd.read_csv(transit_input_dir / observed_BART_county)
    model_BART_county_df = pd.read_csv(output_transit_dir / model_BART_county)
    county_order = [
        "San Francisco",
        "San Mateo",
        "Santa Clara",
        "Contra Costa",
        "Alameda",
        "Total",
    ]
    county_br_day = process_data(
        obs_BART_county,
        model_BART_county_df,
        None,
        "County",
        "Boardings",
        "County",
        "left",
    )
    county_br_day["County"] = pd.Categorical(
        county_br_day["County"], categories=county_order, ordered=True
    )
    county_br_day = county_br_day.sort_values("County")
    dataframe_to_markdown(
        county_br_day,
        file_name=Path(markdown_output_dir / county_br_day_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_br_am = process_data(
        obs_BART_county,
        model_BART_county_df,
        [("TOD", "AM")],
        "County",
        "Boardings",
        "County",
        "left",
    )
    county_br_am["County"] = pd.Categorical(
        county_br_am["County"], categories=county_order, ordered=True
    )
    county_br_am = county_br_am.sort_values("County")
    dataframe_to_markdown(
        county_br_am,
        file_name=Path(markdown_output_dir / county_br_am_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_br_pm = process_data(
        obs_BART_county,
        model_BART_county_df,
        [("TOD", "PM")],
        "County",
        "Boardings",
        "County",
        "left",
    )
    county_br_pm["County"] = pd.Categorical(
        county_br_pm["County"], categories=county_order, ordered=True
    )
    county_br_pm = county_br_pm.sort_values("County")
    dataframe_to_markdown(
        county_br_pm,
        file_name=Path(markdown_output_dir / county_br_pm_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_br_day.to_csv(BART_output_dir / county_br_day_csv, index=False)
    county_br_am.to_csv(BART_output_dir / county_br_am_csv, index=False)
    county_br_pm.to_csv(BART_output_dir / county_br_pm_csv, index=False)
    county_at_day = process_data(
        obs_BART_county,
        model_BART_county_df,
        None,
        "County",
        "Alightings",
        "County",
        "left",
    )
    county_at_day["County"] = pd.Categorical(
        county_at_day["County"], categories=county_order, ordered=True
    )
    county_at_day = county_at_day.sort_values("County")
    dataframe_to_markdown(
        county_at_day,
        file_name=Path(markdown_output_dir / county_at_day_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_at_am = process_data(
        obs_BART_county,
        model_BART_county_df,
        [("TOD", "AM")],
        "County",
        "Alightings",
        "County",
        "left",
    )
    county_at_am["County"] = pd.Categorical(
        county_at_am["County"], categories=county_order, ordered=True
    )
    county_at_am = county_at_am.sort_values("County")
    dataframe_to_markdown(
        county_at_am,
        file_name=Path(markdown_output_dir / county_at_am_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_at_pm = process_data(
        obs_BART_county,
        model_BART_county_df,
        [("TOD", "PM")],
        "County",
        "Alightings",
        "County",
        "left",
    )
    county_at_pm["County"] = pd.Categorical(
        county_at_pm["County"], categories=county_order, ordered=True
    )
    county_at_pm = county_at_pm.sort_values("County")
    dataframe_to_markdown(
        county_at_pm,
        file_name=Path(markdown_output_dir / county_at_pm_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=90,
    )
    county_at_day.to_csv(BART_output_dir / county_at_day_csv, index=False)
    county_at_am.to_csv(BART_output_dir / county_at_am_csv, index=False)
    county_at_pm.to_csv(BART_output_dir / county_at_pm_csv, index=False)

    # BART Screenline
    obs_BART_Screenline = pd.read_csv(transit_input_dir / observed_BART_Screenline)
    model_BART_Screenline_df = pd.read_csv(output_transit_dir / model_BART_Screenline)

    transbay_BART_IB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "Transbay"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    transbay_BART_IB["TOD"] = pd.Categorical(
        transbay_BART_IB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    transbay_BART_IB = transbay_BART_IB.sort_values("TOD")
    transbay_BART_IB = transbay_BART_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_BART_IB,
        file_name=Path(markdown_output_dir / transbay_BART_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_BART_OB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "Transbay"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    transbay_BART_OB["TOD"] = pd.Categorical(
        transbay_BART_OB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    transbay_BART_OB = transbay_BART_OB.sort_values("TOD")
    transbay_BART_OB = transbay_BART_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_BART_OB,
        file_name=Path(markdown_output_dir / transbay_BART_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_BART_OB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "Countyline"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_BART_OB["TOD"] = pd.Categorical(
        Countyline_BART_OB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    Countyline_BART_OB = Countyline_BART_OB.sort_values("TOD")
    Countyline_BART_OB = Countyline_BART_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_BART_OB,
        file_name=Path(markdown_output_dir / Countyline_BART_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_BART_IB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "Countyline"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_BART_IB["TOD"] = pd.Categorical(
        Countyline_BART_IB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    Countyline_BART_IB = Countyline_BART_IB.sort_values("TOD")
    Countyline_BART_IB = Countyline_BART_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_BART_IB,
        file_name=Path(markdown_output_dir / Countyline_BART_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Intra_SF_BART_OB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "SF-San Mateo"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Intra_SF_BART_OB["TOD"] = pd.Categorical(
        Intra_SF_BART_OB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    Intra_SF_BART_OB = Intra_SF_BART_OB.sort_values("TOD")
    Intra_SF_BART_OB = Intra_SF_BART_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Intra_SF_BART_OB,
        file_name=Path(markdown_output_dir / SF_out_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Intra_SF_BART_IB = process_data(
        obs_BART_Screenline,
        model_BART_Screenline_df,
        [("Screenline", "SF-San Mateo"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Intra_SF_BART_IB["TOD"] = pd.Categorical(
        Intra_SF_BART_IB["TOD"], categories=tod_order, ordered=True
    )
    # Sort the DataFrame by the 'TOD' column
    Intra_SF_BART_IB = Intra_SF_BART_IB.sort_values("TOD")
    Intra_SF_BART_IB = Intra_SF_BART_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Intra_SF_BART_IB,
        file_name=Path(markdown_output_dir / SF_in_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )

    transbay_BART_IB[~transbay_BART_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_BART_IB_csv, index=False
    )
    transbay_BART_OB[~transbay_BART_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_BART_OB_csv, index=False
    )
    Countyline_BART_IB[~Countyline_BART_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_BART_IB_csv, index=False
    )
    Countyline_BART_OB[~Countyline_BART_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_BART_OB_csv, index=False
    )
    Intra_SF_BART_IB[~Intra_SF_BART_IB["TOD"].isin(["Total"])].to_csv(
        BART_output_dir / Intra_SF_BART_IB_csv, index=False
    )
    Intra_SF_BART_OB[~Intra_SF_BART_OB["TOD"].isin(["Total"])].to_csv(
        BART_output_dir / Intra_SF_BART_OB_csv, index=False
    )


def process_mkd_screenline(
    transit_input_dir,
    observed_Screenline,
    output_transit_dir,
    model_Screenline,
    markdown_output_dir,
    tod_order,
    Screenline_output_dir,
    transbay_AC_IB_md,
    transbay_AC_OB_md,
    transbay_overall_IB_md,
    transbay_overall_OB_md,
    Countyline_CalTrain_IB_md,
    Countyline_CalTrain_OB_md,
    Countyline_SamTrans_IB_md,
    Countyline_SamTrans_OB_md,
    Countyline_overall_IB_md,
    Countyline_overall_OB_md,
    GG_Transit_IB_md,
    GG_Transit_OB_md,
    GG_Ferry_IB_md,
    GG_Ferry_OB_md,
    GG_overall_IB_md,
    GG_overall_OB_md,
    transbay_AC_IB_csv,
    transbay_AC_OB_csv,
    transbay_overall_IB_csv,
    transbay_overall_OB_csv,
    Countyline_CalTrain_IB_csv,
    Countyline_CalTrain_OB_csv,
    Countyline_SamTrans_IB_csv,
    Countyline_SamTrans_OB_csv,
    Countyline_overall_IB_csv,
    Countyline_overall_OB_csv,
    GG_Transit_IB_csv,
    GG_Transit_OB_csv,
    GG_Ferry_IB_csv,
    GG_Ferry_OB_csv,
    GG_overall_IB_csv,
    GG_overall_OB_csv,
    screenline_overall_ib_csv,
    screenline_overall_ob_csv,
    screenline_overall_ib_md,
    screenline_overall_ob_md
):
    # Valdiation for Screenlines
    obs_Screenline = pd.read_csv(transit_input_dir / observed_Screenline)
    model_Screenline_df = pd.read_csv(output_transit_dir / model_Screenline)
    screenline_overall_ib = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    screenline_overall_ib["TOD"] = pd.Categorical(
        screenline_overall_ib["TOD"], categories=tod_order, ordered=True
    )
    screenline_overall_ib = screenline_overall_ib.sort_values("TOD")
    screenline_overall_ib = screenline_overall_ib.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        screenline_overall_ib,
        file_name=Path(markdown_output_dir / screenline_overall_ib_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    screenline_overall_ob = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    screenline_overall_ob["TOD"] = pd.Categorical(
        screenline_overall_ob["TOD"], categories=tod_order, ordered=True
    )
    screenline_overall_ob = screenline_overall_ob.sort_values("TOD")
    screenline_overall_ob = screenline_overall_ob.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        screenline_overall_ob,
        file_name=Path(markdown_output_dir / screenline_overall_ob_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_AC_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Transbay"), ("Operator", "AC Transit"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    transbay_AC_IB["TOD"] = pd.Categorical(
        transbay_AC_IB["TOD"], categories=tod_order, ordered=True
    )
    transbay_AC_IB = transbay_AC_IB.sort_values("TOD")
    transbay_AC_IB = transbay_AC_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_AC_IB,
        file_name=Path(markdown_output_dir / transbay_AC_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_AC_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Transbay"), ("Operator", "AC Transit"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "right",
    )
    transbay_AC_OB["TOD"] = pd.Categorical(
        transbay_AC_OB["TOD"], categories=tod_order, ordered=True
    )
    transbay_AC_OB = transbay_AC_OB.sort_values("TOD")
    transbay_AC_OB = transbay_AC_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_AC_OB,
        file_name=Path(markdown_output_dir / transbay_AC_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_overall_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Transbay"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    transbay_overall_IB["TOD"] = pd.Categorical(
        transbay_overall_IB["TOD"], categories=tod_order, ordered=True
    )
    transbay_overall_IB = transbay_overall_IB.sort_values("TOD")
    transbay_overall_IB = transbay_overall_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_overall_IB,
        file_name=Path(markdown_output_dir / transbay_overall_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_overall_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Transbay"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    transbay_overall_OB["TOD"] = pd.Categorical(
        transbay_overall_OB["TOD"], categories=tod_order, ordered=True
    )
    transbay_overall_OB = transbay_overall_OB.sort_values("TOD")
    transbay_overall_OB = transbay_overall_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        transbay_overall_OB,
        file_name=Path(markdown_output_dir / transbay_overall_OB_md),
        highlight_rows=[-1],
        center_align_columns=None,
        column_widths=70,
    )
    transbay_overall_IB[~transbay_overall_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_overall_IB_csv, index=False
    )
    transbay_overall_OB[~transbay_overall_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_overall_OB_csv, index=False
    )
    transbay_AC_IB[~transbay_AC_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_AC_IB_csv, index=False
    )
    transbay_AC_OB[~transbay_AC_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / transbay_AC_OB_csv, index=False
    )
    Countyline_CalTrain_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Operator", "CalTrain"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "right",
    )
    Countyline_CalTrain_IB["TOD"] = pd.Categorical(
        Countyline_CalTrain_IB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_CalTrain_IB = Countyline_CalTrain_IB.sort_values("TOD")
    Countyline_CalTrain_IB = Countyline_CalTrain_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_CalTrain_IB,
        file_name=Path(markdown_output_dir / Countyline_CalTrain_IB_md),
        highlight_rows=[-1],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_CalTrain_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Operator", "CalTrain"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "right",
    )
    Countyline_CalTrain_OB["TOD"] = pd.Categorical(
        Countyline_CalTrain_OB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_CalTrain_OB = Countyline_CalTrain_OB.sort_values("TOD")
    Countyline_CalTrain_OB = Countyline_CalTrain_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_CalTrain_OB,
        file_name=Path(markdown_output_dir / Countyline_CalTrain_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_SamTrans_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Operator", "SamTrans"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_SamTrans_IB["TOD"] = pd.Categorical(
        Countyline_SamTrans_IB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_SamTrans_IB = Countyline_SamTrans_IB.sort_values("TOD")
    Countyline_SamTrans_IB = Countyline_SamTrans_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_SamTrans_IB,
        file_name=Path(markdown_output_dir / Countyline_SamTrans_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_SamTrans_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Operator", "SamTrans"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_SamTrans_OB["TOD"] = pd.Categorical(
        Countyline_SamTrans_OB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_SamTrans_OB = Countyline_SamTrans_OB.sort_values("TOD")
    Countyline_SamTrans_OB = Countyline_SamTrans_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_SamTrans_OB,
        file_name=Path(markdown_output_dir / Countyline_SamTrans_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_overall_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_overall_IB["TOD"] = pd.Categorical(
        Countyline_overall_IB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_overall_IB = Countyline_overall_IB.sort_values("TOD")
    Countyline_overall_IB = Countyline_overall_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_overall_IB,
        file_name=Path(markdown_output_dir / Countyline_overall_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    Countyline_overall_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Countyline"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    Countyline_overall_OB["TOD"] = pd.Categorical(
        Countyline_overall_OB["TOD"], categories=tod_order, ordered=True
    )
    Countyline_overall_OB = Countyline_overall_OB.sort_values("TOD")
    Countyline_overall_OB = Countyline_overall_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        Countyline_overall_OB,
        file_name=Path(markdown_output_dir / Countyline_overall_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    screenline_overall_ib[~screenline_overall_ib["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / screenline_overall_ib_csv, index=False
    )
    screenline_overall_ob[~screenline_overall_ob["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / screenline_overall_ob_csv, index=False
    )
    Countyline_CalTrain_IB[~Countyline_CalTrain_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_CalTrain_IB_csv, index=False
    )
    Countyline_CalTrain_OB[~Countyline_CalTrain_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_CalTrain_OB_csv, index=False
    )
    Countyline_SamTrans_IB[~Countyline_SamTrans_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_SamTrans_IB_csv, index=False
    )
    Countyline_SamTrans_OB[~Countyline_SamTrans_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_SamTrans_OB_csv, index=False
    )
    Countyline_overall_IB[~Countyline_overall_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_overall_IB_csv, index=False
    )
    Countyline_overall_OB[~Countyline_overall_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / Countyline_overall_OB_csv, index=False
    )
    GG_Transit_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [
            ("Screenline", "Golden Gate"),
            ("Operator", "Golden Gate Transit"),
            ("Direction", "IB"),
        ],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    GG_Transit_IB["TOD"] = pd.Categorical(
        GG_Transit_IB["TOD"], categories=tod_order, ordered=True
    )
    GG_Transit_IB = GG_Transit_IB.sort_values("TOD")
    GG_Transit_IB = GG_Transit_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_Transit_IB,
        file_name=Path(markdown_output_dir / GG_Transit_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_Transit_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [
            ("Screenline", "Golden Gate"),
            ("Operator", "Golden Gate Transit"),
            ("Direction", "OB"),
        ],
        "TOD",
        "Ridership",
        "TOD",
        "right",
    )
    GG_Transit_OB["TOD"] = pd.Categorical(
        GG_Transit_OB["TOD"], categories=tod_order, ordered=True
    )
    GG_Transit_OB = GG_Transit_OB.sort_values("TOD")
    GG_Transit_OB = GG_Transit_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_Transit_OB,
        file_name=Path(markdown_output_dir / GG_Transit_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_Ferry_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [
            ("Screenline", "Golden Gate"),
            ("Operator", "Golden Gate Ferry"),
            ("Direction", "IB"),
        ],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    GG_Ferry_IB["TOD"] = pd.Categorical(
        GG_Ferry_IB["TOD"], categories=tod_order, ordered=True
    )
    GG_Ferry_IB = GG_Ferry_IB.sort_values("TOD")
    GG_Ferry_IB = GG_Ferry_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_Ferry_IB,
        file_name=Path(markdown_output_dir / GG_Ferry_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_Ferry_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [
            ("Screenline", "Golden Gate"),
            ("Operator", "Golden Gate Ferry"),
            ("Direction", "OB"),
        ],
        "TOD",
        "Ridership",
        "TOD",
        "right",
    )
    GG_Ferry_OB["TOD"] = pd.Categorical(
        GG_Ferry_OB["TOD"], categories=tod_order, ordered=True
    )
    GG_Ferry_OB = GG_Ferry_OB.sort_values("TOD")
    GG_Ferry_OB = GG_Ferry_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_Ferry_OB,
        file_name=Path(markdown_output_dir / GG_Ferry_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_overall_IB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Golden Gate"), ("Direction", "IB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    GG_overall_IB["TOD"] = pd.Categorical(
        GG_overall_IB["TOD"], categories=tod_order, ordered=True
    )
    GG_overall_IB = GG_overall_IB.sort_values("TOD")
    GG_overall_IB = GG_overall_IB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_overall_IB,
        file_name=Path(markdown_output_dir / GG_overall_IB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_overall_OB = process_data(
        obs_Screenline,
        model_Screenline_df,
        [("Screenline", "Golden Gate"), ("Direction", "OB")],
        "TOD",
        "Ridership",
        "TOD",
        "left",
    )
    GG_overall_OB["TOD"] = pd.Categorical(
        GG_overall_OB["TOD"], categories=tod_order, ordered=True
    )
    GG_overall_OB = GG_overall_OB.sort_values("TOD")
    GG_overall_OB = GG_overall_OB.set_index('TOD').reindex(tod_order).reset_index()
    dataframe_to_markdown(
        GG_overall_OB,
        file_name=Path(markdown_output_dir / GG_overall_OB_md),
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=70,
    )
    GG_Transit_IB[~GG_Transit_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_Transit_IB_csv, index=False
    )
    GG_Transit_OB[~GG_Transit_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_Transit_OB_csv, index=False
    )
    GG_Ferry_IB[~GG_Ferry_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_Ferry_IB_csv, index=False
    )
    GG_Ferry_OB[~GG_Ferry_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_Ferry_OB_csv, index=False
    )
    GG_overall_IB[~GG_overall_IB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_overall_IB_csv, index=False
    )
    GG_overall_OB[~GG_overall_OB["TOD"].isin(["Total"])].to_csv(
        Screenline_output_dir / GG_overall_OB_csv, index=False
    )


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    model_run_dir = Path(config["directories"]["model_run"])
    transit_input_dir = Path(config["directories"]["transit_input_dir"])
    MUNI_output_dir = Path(config["directories"]["MUNI_output_dir"])
    markdown_output_dir = Path(config["directories"]["markdown_output_dir"])
    Screenline_output_dir = Path(config["directories"]["Screenline_output_dir"])
    BART_output_dir = Path(config["directories"]["BART_output_dir"])
    observed_BART = Path(config["transit"]["observed_BART"])
    observed_BART_county = Path(config["transit"]["observed_BART_county"])
    observed_BART_Screenline = Path(config["transit"]["observed_BART_Screenline"])
    observed_MUNI_Line = Path(config["transit"]["observed_MUNI_Line"])
    observed_Screenline = Path(config["transit"]["observed_Screenline"])
    observed_NTD = Path(config["transit"]["observed_NTD"])
    model_BART = Path(config["bart"]["model_BART"])
    model_BART_county = Path(config["bart"]["model_BART_county"])
    model_BART_Screenline = Path(config["bart"]["model_BART_Screenline"])
    model_MUNI_Line = Path(config["muni"]["model_MUNI_Line"])
    model_Screenline = Path(config["screenline"]["model_Screenline"])
    transbay_BART_IB_csv = Path(config["screenline"]["transbay_BART_IB_csv"])
    transbay_BART_OB_csv = Path(config["screenline"]["transbay_BART_OB_csv"])
    Countyline_BART_IB_csv = Path(config["screenline"]["Countyline_BART_IB_csv"])
    Countyline_BART_OB_csv = Path(config["screenline"]["Countyline_BART_OB_csv"])
    Intra_SF_BART_IB_csv = Path(config["screenline"]["Intra_SF_BART_IB_csv"])
    Intra_SF_BART_OB_csv = Path(config["screenline"]["Intra_SF_BART_OB_csv"])
    transbay_AC_IB_csv = Path(config["screenline"]["transbay_AC_IB_csv"])
    transbay_AC_OB_csv = Path(config["screenline"]["transbay_AC_OB_csv"])
    transbay_overall_IB_csv = Path(config["screenline"]["transbay_overall_IB_csv"])
    transbay_overall_OB_csv = Path(config["screenline"]["transbay_overall_OB_csv"])
    Countyline_CalTrain_IB_csv = Path(
        config["screenline"]["Countyline_CalTrain_IB_csv"]
    )
    Countyline_CalTrain_OB_csv = Path(
        config["screenline"]["Countyline_CalTrain_OB_csv"]
    )
    Countyline_SamTrans_IB_csv = Path(
        config["screenline"]["Countyline_SamTrans_IB_csv"]
    )
    Countyline_SamTrans_OB_csv = Path(
        config["screenline"]["Countyline_SamTrans_OB_csv"]
    )
    Countyline_overall_IB_csv = Path(config["screenline"]["Countyline_overall_IB_csv"])
    Countyline_overall_OB_csv = Path(config["screenline"]["Countyline_overall_OB_csv"])
    GG_Transit_IB_csv = Path(config["screenline"]["GG_Transit_IB_csv"])
    GG_Transit_OB_csv = Path(config["screenline"]["GG_Transit_OB_csv"])
    GG_Ferry_IB_csv = Path(config["screenline"]["GG_Ferry_IB_csv"])
    GG_Ferry_OB_csv = Path(config["screenline"]["GG_Ferry_OB_csv"])
    GG_overall_IB_csv = Path(config["screenline"]["GG_overall_IB_csv"])
    GG_overall_OB_csv = Path(config["screenline"]["GG_overall_OB_csv"])
    MUNI_ib_day = Path(config["muni"]["MUNI_ib_day"])
    MUNI_ob_day = Path(config["muni"]["MUNI_ob_day"])
    MUNI_ib_am = Path(config["muni"]["MUNI_ib_am"])
    MUNI_ib_pm = Path(config["muni"]["MUNI_ib_pm"])
    MUNI_ob_am = Path(config["muni"]["MUNI_ob_am"])
    MUNI_ob_pm = Path(config["muni"]["MUNI_ob_pm"])
    MUNI_mode_day = Path(config["muni"]["MUNI_mode_day"])
    MUNI_mode_am_md = Path(config["muni"]["MUNI_mode_am_md"])
    MUNI_mode_pm_md = Path(config["muni"]["MUNI_mode_pm_md"])
    MUNI_tod_md = Path(config["muni"]["MUNI_tod_md"])
    MUNI_EB_md = Path(config["muni"]["MUNI_EB_md"])
    MUNI_LB_md = Path(config["muni"]["MUNI_LB_md"])
    MUNI_Rail_md = Path(config["muni"]["MUNI_Rail_md"])
    BART_boarding_allday_md = Path(config["bart"]["BART_boarding_allday_md"])
    BART_boarding_am_md = Path(config["bart"]["BART_boarding_am_md"])
    BART_boarding_pm_md = Path(config["bart"]["BART_boarding_pm_md"])
    BART_at_allday_md = Path(config["bart"]["BART_at_allday_md"])
    BART_at_am_md = Path(config["bart"]["BART_at_am_md"])
    BART_at_pm_md = Path(config["bart"]["BART_at_pm_md"])
    county_br_day_md = Path(config["bart"]["county_br_day_md"])
    county_br_am_md = Path(config["bart"]["county_br_am_md"])
    county_br_pm_md = Path(config["bart"]["county_br_pm_md"])
    county_at_day_md = Path(config["bart"]["county_at_day_md"])
    county_at_am_md = Path(config["bart"]["county_at_am_md"])
    county_at_pm_md = Path(config["bart"]["county_at_pm_md"])
    transbay_BART_IB_md = Path(config["screenline"]["transbay_BART_IB_md"])
    transbay_BART_OB_md = Path(config["screenline"]["transbay_BART_OB_md"])
    Countyline_BART_OB_md = Path(config["screenline"]["Countyline_BART_OB_md"])
    Countyline_BART_IB_md = Path(config["screenline"]["Countyline_BART_IB_md"])
    SF_out_md = Path(config["screenline"]["SF_out_md"])
    SF_in_md = Path(config["screenline"]["SF_in_md"])
    transbay_AC_IB_md = Path(config["screenline"]["transbay_AC_IB_md"])
    transbay_AC_OB_md = Path(config["screenline"]["transbay_AC_OB_md"])
    transbay_overall_IB_md = Path(config["screenline"]["transbay_overall_IB_md"])
    transbay_overall_OB_md = Path(config["screenline"]["transbay_overall_OB_md"])
    Countyline_CalTrain_IB_md = Path(config["screenline"]["Countyline_CalTrain_IB_md"])
    Countyline_CalTrain_OB_md = Path(config["screenline"]["Countyline_CalTrain_OB_md"])
    Countyline_SamTrans_IB_md = Path(config["screenline"]["Countyline_SamTrans_IB_md"])
    Countyline_SamTrans_OB_md = Path(config["screenline"]["Countyline_SamTrans_OB_md"])
    Countyline_overall_IB_md = Path(config["screenline"]["Countyline_overall_IB_md"])
    Countyline_overall_OB_md = Path(config["screenline"]["Countyline_overall_OB_md"])
    GG_Transit_IB_md = Path(config["screenline"]["GG_Transit_IB_md"])
    GG_Transit_OB_md = Path(config["screenline"]["GG_Transit_OB_md"])
    GG_Ferry_IB_md = Path(config["screenline"]["GG_Ferry_IB_md"])
    GG_Ferry_OB_md = Path(config["screenline"]["GG_Ferry_OB_md"])
    GG_overall_IB_md = Path(config["screenline"]["GG_overall_IB_md"])
    GG_overall_OB_md = Path(config["screenline"]["GG_overall_OB_md"])
    screenline_overall_ib_csv = Path(config["screenline"]["screenline_overall_ib_csv"])
    screenline_overall_ob_csv = Path(config["screenline"]["screenline_overall_ob_csv"])
    screenline_overall_ib_md = Path(config["screenline"]["screenline_overall_ib_md"])
    screenline_overall_ob_md = Path(config["screenline"]["screenline_overall_ob_md"])
    valTotal_Submode_md = Path(config["total"]["valTotal_Submode_md"])
    valTotal_Service_md = Path(config["total"]["valTotal_Service_md"])
    valTotal_Operator_md = Path(config["total"]["valTotal_Operator_md"])
    MUNI_OB = Path(config["muni"]["MUNI_OB"])
    MUNI_IB = Path(config["muni"]["MUNI_IB"])
    MUNI_map_IB = Path(config["muni"]["MUNI_map_IB"])
    MUNI_map_OB = Path(config["muni"]["MUNI_map_OB"])
    MUNI_mode = Path(config["muni"]["MUNI_mode"])
    MUNI_mode_am = Path(config["muni"]["MUNI_mode_am"])
    MUNI_mode_pm = Path(config["muni"]["MUNI_mode_pm"])
    MUNI_tod = Path(config["muni"]["MUNI_tod"])
    MUNI_EB = Path(config["muni"]["MUNI_EB"])
    MUNI_LB = Path(config["muni"]["MUNI_LB"])
    MUNI_Rail = Path(config["muni"]["MUNI_Rail"])
    BART_br = Path(config["bart"]["BART_br"])
    BART_br_am = Path(config["bart"]["BART_br_am"])
    BART_br_pm = Path(config["bart"]["BART_br_pm"])
    BART_at = Path(config["bart"]["BART_at"])
    BART_at_am = Path(config["bart"]["BART_at_am"])
    BART_at_pm = Path(config["bart"]["BART_at_pm"])
    BART_boarding_allday_csv = Path(config["bart"]["BART_boarding_allday_csv"])
    BART_at_allday_csv = Path(config["bart"]["BART_at_allday_csv"])
    county_br_day_csv = Path(config["bart"]["county_br_day_csv"])
    county_br_am_csv = Path(config["bart"]["county_br_am_csv"])
    county_br_pm_csv = Path(config["bart"]["county_br_pm_csv"])
    county_at_day_csv = Path(config["bart"]["county_at_day_csv"])
    county_at_am_csv = Path(config["bart"]["county_at_am_csv"])
    county_at_pm_csv = Path(config["bart"]["county_at_pm_csv"])
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    tod_order = ["EA", "AM", "MD", "PM", "EV", "Total"]

    process_mkd_muni(
        transit_input_dir,
        observed_MUNI_Line,
        output_transit_dir,
        model_MUNI_Line,
        markdown_output_dir,
        MUNI_output_dir,
        MUNI_ib_day,
        MUNI_ob_day,
        MUNI_ib_am,
        MUNI_ib_pm,
        MUNI_ob_am,
        MUNI_ob_pm,
        MUNI_mode_day,
        MUNI_mode,
        MUNI_mode_am_md,
        MUNI_mode_am,
        MUNI_mode_pm_md,
        MUNI_mode_pm,
        MUNI_tod_md,
        MUNI_tod,
        MUNI_EB_md,
        MUNI_EB,
        MUNI_LB_md,
        MUNI_LB,
        MUNI_Rail_md,
        MUNI_Rail,
        MUNI_IB,
        MUNI_OB,
    )
    process_mkd_bart(
        transit_input_dir,
        observed_BART,
        output_transit_dir,
        model_BART,
        markdown_output_dir,
        BART_output_dir,
        observed_BART_county,
        model_BART_county,
        observed_BART_Screenline,
        Screenline_output_dir,
        tod_order,
        BART_boarding_allday_md,
        BART_boarding_am_md,
        BART_boarding_pm_md,
        BART_at_allday_md,
        BART_at_am_md,
        BART_at_pm_md,
        BART_boarding_allday_csv,
        BART_at_allday_csv,
        county_br_day_csv,
        county_br_am_csv,
        county_br_pm_csv,
        county_at_day_csv,
        county_at_am_csv,
        county_at_pm_csv,
        model_BART_Screenline,
        county_br_day_md,
        county_br_am_md,
        county_br_pm_md,
        county_at_day_md,
        county_at_am_md,
        county_at_pm_md,
        transbay_BART_IB_md,
        transbay_BART_OB_md,
        Countyline_BART_OB_md,
        Countyline_BART_IB_md,
        SF_out_md,
        SF_in_md,
        transbay_BART_IB_csv,
        transbay_BART_OB_csv,
        Countyline_BART_IB_csv,
        Countyline_BART_OB_csv,
        Intra_SF_BART_IB_csv,
        Intra_SF_BART_OB_csv
    )
    process_mkd_screenline(
        transit_input_dir,
        observed_Screenline,
        output_transit_dir,
        model_Screenline,
        markdown_output_dir,
        tod_order,
        Screenline_output_dir,
        transbay_AC_IB_md,
        transbay_AC_OB_md,
        transbay_overall_IB_md,
        transbay_overall_OB_md,
        Countyline_CalTrain_IB_md,
        Countyline_CalTrain_OB_md,
        Countyline_SamTrans_IB_md,
        Countyline_SamTrans_OB_md,
        Countyline_overall_IB_md,
        Countyline_overall_OB_md,
        GG_Transit_IB_md,
        GG_Transit_OB_md,
        GG_Ferry_IB_md,
        GG_Ferry_OB_md,
        GG_overall_IB_md,
        GG_overall_OB_md,
        transbay_AC_IB_csv,
        transbay_AC_OB_csv,
        transbay_overall_IB_csv,
        transbay_overall_OB_csv,
        Countyline_CalTrain_IB_csv,
        Countyline_CalTrain_OB_csv,
        Countyline_SamTrans_IB_csv,
        Countyline_SamTrans_OB_csv,
        Countyline_overall_IB_csv,
        Countyline_overall_OB_csv,
        GG_Transit_IB_csv,
        GG_Transit_OB_csv,
        GG_Ferry_IB_csv,
        GG_Ferry_OB_csv,
        GG_overall_IB_csv,
        GG_overall_OB_csv,
        screenline_overall_ib_csv,
        screenline_overall_ob_csv,
        screenline_overall_ib_md,
        screenline_overall_ob_md
    )
