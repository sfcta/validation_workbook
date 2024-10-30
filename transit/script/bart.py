import pandas as pd
import tomllib
from utils import read_dbf_and_groupby_sum, read_transit_assignments, time_periods

station_locations = {
    "downtown": ["CIVC", "POWL", "MONT", "EMBR"],
    "not_downtown": ["GLEN", "BALB", "24TH", "16TH"],
}


def process_bart_data(combined_gdf, transit_input_dir, station_node_match):
    # Process BART data for different routes and columns
    nodes = pd.read_csv(transit_input_dir / station_node_match)
    nodes = nodes[["Station", "Node", "County"]]
    # AB_BRDA represents the boarding ridership from A to B
    bart_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "BART", ["A", "TOD"], "AB_BRDA"
    )
    ebart_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "EBART", ["A", "TOD"], "AB_BRDA"
    )
    oac_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "OAC", ["A", "TOD"], "AB_BRDA"
    )

    # AB_XITA represents the alighting ridership from A to B
    bart_alighting = read_dbf_and_groupby_sum(
        combined_gdf, "BART", ["A", "TOD"], "AB_XITA"
    )
    ebart_alighting = read_dbf_and_groupby_sum(
        combined_gdf, "EBART", ["A", "TOD"], "AB_XITA"
    )
    oac_alighting = read_dbf_and_groupby_sum(
        combined_gdf, "OAC", ["A", "TOD"], "AB_XITA"
    )

    # Concatenate and group data
    bart_nodea = pd.concat([bart_boarding, ebart_boarding, oac_boarding])
    bart_nodea = bart_nodea.groupby(["A", "TOD"])["AB_BRDA"].sum().reset_index()
    bart_nodea.columns = ["Node", "TOD", "AB_BRDA"]

    bart_nodeb = pd.concat([bart_alighting, ebart_alighting, oac_alighting])
    bart_nodeb = bart_nodeb.groupby(["A", "TOD"])["AB_XITA"].sum().reset_index()
    bart_nodeb.columns = ["Node", "TOD", "AB_XITA"]

    # Merge with other dataframes
    bart_nodea = pd.merge(bart_nodea, nodes, on=["Node"], how="left")
    bart = pd.merge(bart_nodea, bart_nodeb, on=["Node", "TOD"], how="right")

    # Add columns and rearrange columns
    bart["Key"] = bart["Station"] + bart["TOD"]
    bart.columns = [
        "Node",
        "TOD",
        "Boardings",
        "Station",
        "County",
        "Alightings",
        "Key",
    ]
    bart = bart[["Node", "Station", "County", "TOD", "Key", "Boardings", "Alightings"]]

    return bart


def process_bart_county(
    combined_gdf,
    output_transit_dir,
    transit_input_dir,
    station_node_match,
    model_bart_county,
    model_bart,
):
    bart_county = process_bart_data(combined_gdf, transit_input_dir, station_node_match)

    bart_model = bart_county[["Station", "TOD", "Key", "Boardings", "Alightings"]]
    bart_model = bart_model.sort_values(by="Key").reset_index(drop=True)
    bart_model.to_csv(output_transit_dir / model_bart, index=False)

    bart_county = (
        bart_county.groupby(["County", "TOD"])[["Boardings", "Alightings"]]
        .sum()
        .reset_index()
    )
    bart_county.to_csv(output_transit_dir / model_bart_county, index=False)


def process_bart_screenline_data(combined_gdf, A, B):
    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    bart_screenline = read_dbf_and_groupby_sum(
        combined_gdf, "BART", ["A", "B", "TOD"], "AB_VOL"
    )

    # Filter rows for IB (16510 to 16511)
    IB = bart_screenline[
        (bart_screenline["A"] == A) & (bart_screenline["B"] == B)
    ].copy()
    IB["Direction"] = "IB"

    # Filter rows for OB (16511 to 16510)
    OB = bart_screenline[
        (bart_screenline["A"] == B) & (bart_screenline["B"] == A)
    ].copy()
    OB["Direction"] = "OB"

    # Concatenate IB and OB DataFrames
    result = pd.concat([IB, OB])

    return result


def bart_screenline_concat(combined_gdf, A, B, screenline:str):
    # Concatenate the DataFrames
    bart_screenline = process_bart_screenline_data(combined_gdf, A, B)

    # Add the 'Screenline' column with 'Countyline'
    bart_screenline["Screenline"] = screenline

    # Create the 'Key' column by combining 'Screenline', 'Direction', and 'TOD'
    bart_screenline["Key"] = (
        bart_screenline["Screenline"]
        + bart_screenline["Direction"]
        + bart_screenline["TOD"]
    )

    # Rename the 'AB_VOL' column to 'Ridership'
    bart_screenline = bart_screenline.rename(columns={"AB_VOL": "Ridership"})

    # Select and reorder columns
    bart_screenline = bart_screenline[
        ["Screenline", "Direction", "TOD", "Key", "Ridership"]
    ]

    # Sort by 'Key' and reset the index
    bart_screenline = bart_screenline.sort_values(by="Key").reset_index(drop=True)

    return bart_screenline


def determineScreenline(x):
    if (x["A_Screenline"] == "not_downtown") & (x["B_Screenline"] == "downtown"):
        return "IB"
    elif (x["A_Screenline"] == "downtown") & (x["B_Screenline"] == "not_downtown"):
        return "OB"
    else:
        return False


def process_bart_sf(combined_gdf, transit_input_dir, station_node_match):
    nodes = pd.read_csv(transit_input_dir / station_node_match)
    nodes = nodes[["Station", "Node", "County"]]

    lines = ["BART", "EBART", "OAC"]
    station_to_label = {
        station: label
        for label, stations in station_locations.items()
        for station in stations
    }
    relevant_stations = (
        station_locations["downtown"] + station_locations["not_downtown"]
    )

    # Read, group, and sum data for each line
    dfs = [
        read_dbf_and_groupby_sum(combined_gdf, line, ["A", "B", "TOD"], "AB_VOL")
        for line in lines
    ]

    # Concatenate results
    intra = pd.concat(dfs)

    # Mapping from Node and Station DataFrames
    node_to_station = dict(zip(nodes["Node"], nodes["Station"]))
    intra["A_station"] = intra["A"].map(node_to_station)
    intra["B_station"] = intra["B"].map(node_to_station)
    intra = intra[
        intra["A_station"].isin(relevant_stations)
        & intra["B_station"].isin(relevant_stations)
    ]

    # Apply custom station labeling and direction determination
    intra["A_Screenline"] = intra["A_station"].apply(lambda x: station_to_label[x])
    intra["B_Screenline"] = intra["B_station"].apply(lambda x: station_to_label[x])
    intra["Direction"] = intra.apply(lambda x: determineScreenline(x), axis=1)
    intra = intra[intra["Direction"] != False]

    # Set time of day and select final columns
    intra = intra[["Direction", "TOD", "AB_VOL"]]
    # 2019 validation uses 'Intra-SF'
    intra["Screenline"] = "SF-San Mateo"
    intra["Key"] = intra["Screenline"] + intra["Direction"] + intra["TOD"]
    intra.columns = ["Direction", "TOD", "Ridership", "Screenline", "Key"]
    intra = intra[["Screenline", "Direction", "TOD", "Key", "Ridership"]]
    intra = intra.sort_values(by=["Direction", "TOD"]).reset_index(drop=True)

    return intra


def process_bart_screenline(
    combined_gdf,
    output_transit_dir,
    transit_input_dir,
    station_node_match,
    model_bart_Screenline,
    transbay_node,
    countyline_node,
):
    # Transbay
    bart_screenline_tb = bart_screenline_concat(
        combined_gdf, transbay_node[0], transbay_node[1], "Transbay"
    )

    # Countyline
    bart_screenline_ct = bart_screenline_concat(
        combined_gdf, countyline_node[0], countyline_node[1], "Countyline"
    )

    # Intra-sf: within SF -- Between downtown stations
    bart_sf = process_bart_sf(combined_gdf, transit_input_dir, station_node_match)
    bart_screenline = pd.concat(
        [bart_screenline_tb, bart_screenline_ct, bart_sf], ignore_index=True
    )
    bart_screenline.to_csv(output_transit_dir / model_bart_Screenline, index=False)


def process_bart_model_outputs(
    combined_gdf,
    output_transit_dir,
    transit_input_dir,
    station_node_match,
    model_bart_Screenline,
    model_bart_county,
    model_bart,
    transbay_node,
    countyline_node,
):
    process_bart_screenline(
        combined_gdf,
        output_transit_dir,
        transit_input_dir,
        station_node_match,
        model_bart_Screenline,
        transbay_node,
        countyline_node,
    )
    process_bart_county(
        combined_gdf,
        output_transit_dir,
        transit_input_dir,
        station_node_match,
        model_bart_county,
        model_bart,
    )


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    model_run_dir = config["directories"]["model_run"]
    model_bart = config["bart"]["model_BART"]
    model_bart_county = config["bart"]["model_BART_county"]
    model_bart_Screenline = config["bart"]["model_BART_Screenline"]
    station_node_match = config["transit"]["station_node_match"]
    transit_input_dir = config["directories"]["transit_input_dir"]
    transbay_node = config["screenline"]["transbay_node"]
    countyline_node = config["screenline"]["countyline_node"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    combined_gdf = read_transit_assignments(model_run_dir, time_periods)

    process_bart_model_outputs(
        combined_gdf,
        output_transit_dir,
        transit_input_dir,
        station_node_match,
        model_run_dir,
        output_dir,
        model_bart_Screenline,
        model_bart_county,
        model_bart,
    )
