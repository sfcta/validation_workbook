import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import tomllib
from shapely.geometry import LineString, Point
from transit.utils import (
    format_dataframe,
    read_dbf_and_groupby_sum,
    read_transit_assignments,
    time_periods,
)


def sort_dataframe_by_mixed_column(df, column_name):
    """
    Sorts a DataFrame by a column that contains mixed numeric-text values. Numeric prefixes are sorted numerically,
    and text entries are sorted alphabetically. Null values are sorted to the bottom.

    Parameters:
    df (DataFrame): The DataFrame to sort.
    column_name (str): The name of the column with mixed numeric-text values to sort by.

    Returns:
    DataFrame: The sorted DataFrame.
    """
    # Extract the numeric part and the text part into separate columns
    split_columns = df[column_name].str.extract(r"(?:(\d+) - )?(.*)")

    # Convert the numeric part to integers, using a placeholder for sorting null values
    placeholder_for_sorting = 999999  # Use a large number to sort nulls last
    split_columns[0] = (
        pd.to_numeric(split_columns[0], errors="coerce")
        .fillna(placeholder_for_sorting)
        .astype(int)
    )

    # Sort by the numeric part and then by the text part, ensuring null values go to the bottom
    df_sorted = df.loc[split_columns.sort_values(by=[0, 1], na_position="last").index]

    # Reset the index of the sorted DataFrame
    df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted


def split_dataframe_by_name_ending(df, column_name):
    """
    Splits a DataFrame into two based on the last character of a specified column's values.

    Parameters:
    df (DataFrame): The DataFrame to split.
    column_name (str): The name of the column to check for the ending character.

    Returns:
    tuple: A tuple containing two DataFrames. The first with names ending in 'I', the second with names ending in 'O'.
    """
    # Filter the DataFrame for entries ending with 'I'
    df_ending_i = df[df[column_name].str.endswith("I")]

    # Filter the DataFrame for entries ending with 'O'
    df_ending_o = df[df[column_name].str.endswith("O")]

    return df_ending_i, df_ending_o


def map_name_to_direction(name):
    if name.endswith("I"):
        return "IB"
    elif name.endswith("O"):
        return "OB"
    else:
        return None  # Return None for other cases


def concat_ordered_geometries(group):
    """ Function to concatenate ordered geometries """
    sorted_geoms = group.sort_values(by="SEQ")["geometry"]
    return LineString([pt for geom in sorted_geoms for pt in geom.coords])


def create_station_df(transit_input_dir, station_node_match):
    df_station_name = pd.read_csv(transit_input_dir / station_node_match)
    df_station_name["geometry"] = df_station_name.apply(
        lambda row: Point(row["x"], row["y"]), axis=1
    )
    station = gpd.GeoDataFrame(df_station_name, geometry="geometry")
    return station


def process_muni_map(
    combined_gdf,
    output_transit_dir,
    muni_output_dir,
    shp_file_dir,
    FREEFLOW_SHP,
    model_MUNI_Line,
    muni_ib,
    muni_ob,
    MUNI_OB,
    MUNI_IB,
    MUNI_map_IB,
    MUNI_map_OB,
):
    MUNI = read_dbf_and_groupby_sum(
        combined_gdf, "SF MUNI", ["FULLNAME", "NAME", "AB", "SEQ"], "AB_BRDA"
    )
    MUNI = sort_dataframe_by_mixed_column(MUNI, "FULLNAME")
    MUNI["Direction"] = MUNI["NAME"].apply(map_name_to_direction)
    MUNI_day = MUNI.groupby(
        ["FULLNAME", "NAME", "AB", "SEQ", "Direction"], as_index=False
    )["AB_BRDA"].sum()
    MUNI_day = MUNI_day.rename(columns={"NAME": "Name"})
    model_MUNI_line_df = pd.read_csv(output_transit_dir / model_MUNI_Line)
    MUNI_map = model_MUNI_line_df.merge(MUNI_day, on="Name", how="left")
    MUNI_map = MUNI_map[["Name", "Line", "AB", "SEQ"]]
    MUNI_map["Direction"] = MUNI_map["Name"].apply(map_name_to_direction)

    MUNI_IB_df = pd.read_csv(muni_output_dir / MUNI_IB)
    MUNI_map_IN = MUNI_map[MUNI_map["Direction"] == "IB"]
    MUNI_map_IN = MUNI_map_IN.rename(columns={"Line": "Route"})
    MUNI_IB_df = MUNI_IB_df.merge(MUNI_map_IN, on="Route", how="left")

    MUNI_map_OUT = MUNI_map[MUNI_map["Direction"] == "OB"]
    MUNI_map_OUT = MUNI_map_OUT.rename(columns={"Line": "Route"})
    MUNI_OB_df = pd.read_csv(muni_output_dir / MUNI_OB)
    MUNI_OB_df = MUNI_OB_df.merge(MUNI_map_OUT, on="Route", how="left")
    # GEO info
    freeflow = gpd.read_file(FREEFLOW_SHP)
    freeflow.crs = "epsg:2227"
    freeflow = freeflow.to_crs(epsg=4236)
    node_geo = freeflow[["A", "B", "AB", "geometry"]].copy()
    MUNI_IB_df = MUNI_IB_df[
        [
            "Route",
            "Name",
            "Observed",
            "Modeled",
            "Diff",
            "Percentage Diff",
            "AB",
            "SEQ",
            "Direction",
        ]
    ]
    muni_ib_df = (
        MUNI_IB_df.merge(node_geo, on="AB", how="left").dropna().drop_duplicates()
    )
    muni_ib_geo = gpd.GeoDataFrame(muni_ib_df, geometry="geometry")
    # Apply aggregation function using `apply` instead of `agg`
    aggregated_muni_ib = (
        muni_ib_geo.groupby("Name")
        .apply(
            lambda x: pd.Series(
                {
                    "Route": x["Route"].iloc[0],
                    "Observed": x["Observed"].iloc[0],
                    "Modeled": x["Modeled"].iloc[0],
                    "Diff": x["Diff"].iloc[0],
                    "Percentage Diff": x["Percentage Diff"].iloc[0],
                    "AB": x["AB"].iloc[0],
                    "Direction": x["Direction"].iloc[0],
                    "geometry": concat_ordered_geometries(x),
                }
            )
        )
        .reset_index()
    )

    aggregated_muni_ib = (
        aggregated_muni_ib.groupby("Route")
        .apply(
            lambda x: pd.Series(
                {
                    "Observed": x["Observed"].iloc[0],
                    "Modeled": x["Modeled"].iloc[0],
                    "Diff": x["Diff"].iloc[0],
                    "Percentage Diff": x["Percentage Diff"].iloc[0],
                    "AB": x["AB"].iloc[0],
                    "Direction": x["Direction"].iloc[0],
                    "geometry": x["geometry"].iloc[0],
                }
            )
        )
        .reset_index()
    )

    # Convert to GeoDataFrame
    aggregated_muni_ib = gpd.GeoDataFrame(aggregated_muni_ib, geometry="geometry")
    aggregated_muni_ib.to_file(shp_file_dir / muni_ib)
    MUNI_map_IB_df = aggregated_muni_ib[
        ["Route", "Observed", "Modeled", "Diff", "Percentage Diff", "Direction"]
    ].copy()
    MUNI_map_IB_df["Percentage Diff"] = pd.to_numeric(
        MUNI_map_IB_df["Percentage Diff"].str.replace("%", "").str.strip(),
        errors="coerce",
    )
    MUNI_map_IB_df["Percentage Diff"] = MUNI_map_IB_df["Percentage Diff"] / 100
    # List of columns to convert
    columns_to_convert = ["Observed", "Diff", "Modeled"]
    for column in columns_to_convert:
        MUNI_map_IB_df[column] = pd.to_numeric(
            MUNI_map_IB_df[column].str.replace(",", "").str.strip(), errors="coerce"
        )
    MUNI_map_IB_df = MUNI_map_IB_df[
        ["Route", "Observed", "Modeled", "Diff", "Percentage Diff", "Direction"]
    ]
    MUNI_map_IB_df = MUNI_map_IB_df.drop_duplicates()
    MUNI_map_IB_df.to_csv(muni_output_dir / MUNI_map_IB, index=False)

    MUNI_OB_df = MUNI_OB_df[
        [
            "Route",
            "Name",
            "Observed",
            "Modeled",
            "Diff",
            "Percentage Diff",
            "AB",
            "SEQ",
            "Direction",
        ]
    ]
    muni_ob_df = (
        MUNI_OB_df.merge(node_geo, on="AB", how="left").dropna().drop_duplicates()
    )
    muni_ob_geo = gpd.GeoDataFrame(muni_ob_df, geometry="geometry")
    # Apply aggregation function using `apply` instead of `agg`
    aggregated_muni_ob = (
        muni_ob_geo.groupby("Name")
        .apply(
            lambda x: pd.Series(
                {
                    "Route": x["Route"].iloc[0],
                    "Observed": x["Observed"].iloc[0],
                    "Modeled": x["Modeled"].iloc[0],
                    "Diff": x["Diff"].iloc[0],
                    "Percentage Diff": x["Percentage Diff"].iloc[0],
                    "AB": x["AB"].iloc[0],
                    "Direction": x["Direction"].iloc[0],
                    "geometry": concat_ordered_geometries(x),
                }
            )
        )
        .reset_index()
    )

    aggregated_muni_ob = (
        aggregated_muni_ob.groupby("Route")
        .apply(
            lambda x: pd.Series(
                {
                    "Observed": x["Observed"].iloc[0],
                    "Modeled": x["Modeled"].iloc[0],
                    "Diff": x["Diff"].iloc[0],
                    "Percentage Diff": x["Percentage Diff"].iloc[0],
                    "AB": x["AB"].iloc[0],
                    "Direction": x["Direction"].iloc[0],
                    "geometry": x["geometry"].iloc[0],
                }
            )
        )
        .reset_index()
    )

    aggregated_muni_ob.to_file(shp_file_dir / muni_ob)
    MUNI_map_OB_df = aggregated_muni_ob[
        ["Route", "Observed", "Modeled", "Diff", "Percentage Diff", "Direction"]
    ].copy()
    MUNI_map_OB_df["Percentage Diff"] = pd.to_numeric(
        MUNI_map_OB_df["Percentage Diff"].str.replace("%", "").str.strip(),
        errors="coerce",
    )
    MUNI_map_OB_df["Percentage Diff"] = MUNI_map_OB_df["Percentage Diff"] / 100
    for column in columns_to_convert:
        MUNI_map_OB_df[column] = pd.to_numeric(
            MUNI_map_OB_df[column].str.replace(",", "").str.strip(), errors="coerce"
        )
    MUNI_map_OB_df = MUNI_map_OB_df[
        ["Route", "Observed", "Modeled", "Diff", "Percentage Diff", "Direction"]
    ]
    MUNI_map_OB_df = MUNI_map_OB_df.drop_duplicates()
    MUNI_map_OB_df.to_csv(muni_output_dir / MUNI_map_OB, index=False)


def BART_map(
    type,
    group_by,
    TOD,
    obs_BART_line,
    model_BART_line,
    csv,
    shp,
    station,
    bart_output_dir,
    shp_file_dir,
):
    obs_condition = pd.Series([True] * len(obs_BART_line))
    model_condition = pd.Series([True] * len(model_BART_line))
    # Processing observed data
    BART_obs = obs_BART_line[obs_condition].groupby(group_by)[type].sum().reset_index()
    BART_obs.rename(columns={type: "Observed"}, inplace=True)

    # Processing modeled data
    BART_model = (
        model_BART_line[model_condition].groupby(group_by)[type].sum().reset_index()
    )
    BART_model.rename(columns={type: "Modeled"}, inplace=True)

    BART = pd.merge(BART_obs, BART_model, on=group_by, how="left")
    BART["Diff"] = BART["Modeled"] - BART["Observed"]
    BART["Percentage Diff"] = BART["Diff"] / BART["Observed"]
    BART["ABS Percentage Diff"] = abs(BART["Percentage Diff"])
    BART["ABS Diff"] = abs(BART["Diff"])
    if TOD is not None:
        BART = BART[BART["TOD"] == TOD].copy()
    BART.to_csv(os.path.join(bart_output_dir, csv), index=False)
    BART_2 = BART.copy()
    BART_2["Percentage Diff"] = BART_2["Percentage Diff"] * 100
    numeric_cols = ["Observed", "Modeled", "Diff"]
    BART_map = format_dataframe(
        BART_2, numeric_columns=numeric_cols, percentage_columns=["Percentage Diff"]
    )
    BART_map = BART_map.merge(station, on="Station", how="right")
    BART_map = gpd.GeoDataFrame(BART_map, geometry="geometry")
    BART_map.crs = "epsg:2227"
    BART_map = BART_map.to_crs(epsg=4236)
    BART_map.to_file(os.path.join(shp_file_dir, shp))


def process_bart_map(
    bart_output_dir,
    transit_input_dir,
    output_transit_dir,
    observed_BART,
    model_BART,
    shp_file_dir,
    BART_br,
    BART_br_map,
    BART_br_pm,
    BART_br_map_pm,
    BART_br_am,
    BART_br_map_am,
    BART_at,
    BART_at_map,
    BART_at_am,
    BART_at_map_am,
    BART_at_pm,
    BART_at_map_pm,
    station_node_match,
):
    # BART
    station = create_station_df(transit_input_dir, station_node_match)
    obs_BART_line = pd.read_csv(transit_input_dir / observed_BART)
    model_BART_line = pd.read_csv(output_transit_dir / model_BART)

    BART_map(
        "Boardings",
        ["Station"],
        None,
        obs_BART_line,
        model_BART_line,
        BART_br,
        BART_br_map,
        station,
        bart_output_dir,
        shp_file_dir,
    )
    BART_map(
        "Boardings",
        ["Station", "TOD"],
        "AM",
        obs_BART_line,
        model_BART_line,
        BART_br_am,
        BART_br_map_am,
        station,
        bart_output_dir,
        shp_file_dir,
    )
    BART_map(
        "Boardings",
        ["Station", "TOD"],
        "PM",
        obs_BART_line,
        model_BART_line,
        BART_br_pm,
        BART_br_map_pm,
        station,
        bart_output_dir,
        shp_file_dir,
    )
    BART_map(
        "Alightings",
        ["Station"],
        None,
        obs_BART_line,
        model_BART_line,
        BART_at,
        BART_at_map,
        station,
        bart_output_dir,
        shp_file_dir,
    )
    BART_map(
        "Alightings",
        ["Station", "TOD"],
        "AM",
        obs_BART_line,
        model_BART_line,
        BART_at_am,
        BART_at_map_am,
        station,
        bart_output_dir,
        shp_file_dir,
    )
    BART_map(
        "Alightings",
        ["Station", "TOD"],
        "PM",
        obs_BART_line,
        model_BART_line,
        BART_at_pm,
        BART_at_map_pm,
        station,
        bart_output_dir,
        shp_file_dir,
    )


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    process_muni_map(
        combined_gdf,
        output_transit_dir,
        muni_output_dir,
        shp_file_dir,
        FREEFLOW_SHP,
        model_MUNI_Line,
        muni_ib_shp,
        muni_ob_shp,
        MUNI_OB,
        MUNI_IB,
        MUNI_map_IB,
        MUNI_map_OB,
    )
    process_bart_map(
        bart_output_dir,
        transit_input_dir,
        output_transit_dir,
        observed_BART,
        model_BART,
        shp_file_dir,
        BART_br,
        BART_br_map,
        BART_br_pm,
        BART_br_map_pm,
        BART_br_am,
        BART_br_map_am,
        BART_at,
        BART_at_map,
        BART_at_am,
        BART_at_map_am,
        BART_at_pm,
        BART_at_map_pm,
        station_node_match,
    )
