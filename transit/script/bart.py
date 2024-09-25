from pathlib import Path

import pandas as pd
import tomllib
from transit_function import read_dbf_and_groupby_sum, read_transit_assignments


def read_nodes(model_run_dir):
    filepath = Path(model_run_dir) / "nodes.xls"
    return pd.read_excel(filepath, header=None, names=["Node", "Node Name"])


station_dict = {
    "Oakland City Center BART": "12TH",
    "16th/Mission BART": "16TH",
    "19th St Oakland BART": "19TH",
    "24th/Mission BART": "24TH",
    "Hillcrest eBART": "ANTC",
    "Ashby BART": "ASHB",
    "Balboa Park BART": "BALB",
    "Bay Fair BART": "BAYF",
    "Castro Valley BART": "CAST",
    "Civic Center BART": "CIVC",
    "Colma BART": "COLM",
    "Coliseum OAK BART": "COLS",
    "Coliseium OAC": "COLS",
    "Concord BART": "CONC",
    "Daly City BART": "DALY",
    "Downtown Berkeley BART": "DBRK",
    "El Cerrito del Norte BART": "DELN",
    "Dublin/Pleasanton BART": "DUBL",
    "Embarcadero BART": "EMBR",
    "Fremont BART": "FRMT",
    "Fruitvale BART": "FTVL",
    "Glen Park BART": "GLEN",
    "Hayward BART": "HAYW",
    "Lafayette BART": "LAFY",
    "Lake Merritt BART": "LAKE",
    "MacArthur BART": "MCAR",
    "Millbrae BART": "MLBR",
    "Montgomery BART": "MONT",
    "North Berkeley BART": "NBRK",
    "North Concord BART": "NCON",
    "Oakland Airport OAC": "OAKL",
    "Orinda BART": "ORIN",
    "Somersville Road eBART": "PCTR",
    "Pleasant Hill BART": "PHIL",
    "Pittsburg/Bay Point BART": "PITT",
    "El Cerrito Plaza BART": "PLZA",
    "Powell BART": "POWL",
    "Richmond BART": "RICH",
    "Rockridge BART": "ROCK",
    "San Leandro BART": "SANL",
    "San Bruno BART": "SBRN",
    "SFO BART": "SFIA",
    "S Hayward BART": "SHAY",
    "South SF BART": "SSAN",
    "Union City BART": "UCTY",
    "Warm Springs BART": "WARM",
    "Walnut Creek BART": "WCRK",
    "West Dublin BART": "WDUB",
    "W Oakland BART": "WOAK",
}

counties = {
    "San Francisco": ["EMBR", "CIVC", "24TH", "MONT", "POWL", "GLEN", "16TH", "BALB"],
    "San Mateo": ["DALY", "COLM", "SSAN", "SBRN", "SFIA", "MLBR"],
    "Contra Costa": [
        "RICH",
        "ORIN",
        "LAFY",
        "WCRK",
        "CONC",
        "NCON",
        "PITT",
        "ANTC",
        "DELN",
        "PHIL",
        "PCTR",
        "PLZA",
    ],
    "Alameda": [
        "WOAK",
        "12TH",
        "19TH",
        "MCAR",
        "ASHB",
        "DUBL",
        "WDUB",
        "CAST",
        "WARM",
        "UCTY",
        "SHAY",
        "HAYW",
        "BAYF",
        "SANL",
        "OAKL",
        "COLS",
        "FTVL",
        "LAKE",
        "ROCK",
        "DBRK",
        "NBRK",
        "FRMT",
    ],
    "Santa Clara": [],  # Add stations for Santa Clara if available
}

station_locations = {
    "downtown": ["CIVC", "POWL", "MONT", "EMBR"],
    "not_downtown": ["GLEN", "BALB", "24TH", "16TH"],
}

# def station_name():
#     station_name = {
#         "Station": [
#             "12TH",
#             "16TH",
#             "19TH",
#             "24TH",
#             "ANTC",
#             "ASHB",
#             "BALB",
#             "BAYF",
#             "CAST",
#             "CIVC",
#             "COLM",
#             "COLS",
#             "CONC",
#             "DALY",
#             "DBRK",
#             "DELN",
#             "DUBL",
#             "EMBR",
#             "FRMT",
#             "FTVL",
#             "GLEN",
#             "HAYW",
#             "LAFY",
#             "LAKE",
#             "MCAR",
#             "MLBR",
#             "MONT",
#             "NBRK",
#             "NCON",
#             "OAKL",
#             "ORIN",
#             "PCTR",
#             "PHIL",
#             "PITT",
#             "PLZA",
#             "POWL",
#             "RICH",
#             "ROCK",
#             "SANL",
#             "SBRN",
#             "SFIA",
#             "SHAY",
#             "SSAN",
#             "UCTY",
#             "WARM",
#             "WCRK",
#             "WDUB",
#             "WOAK",
#         ],
#         "Node": [
#             16509,
#             16515,
#             16508,
#             16516,
#             15231,
#             16525,
#             16518,
#             16530,
#             16537,
#             16514,
#             16539,
#             16532,
#             16501,
#             16519,
#             16523,
#             16521,
#             16538,
#             16511,
#             16526,
#             16533,
#             16517,
#             16529,
#             16504,
#             16534,
#             16507,
#             16543,
#             16512,
#             16524,
#             16535,
#             16000,
#             16505,
#             15230,
#             16502,
#             16536,
#             16522,
#             16513,
#             16520,
#             16506,
#             16531,
#             16541,
#             16542,
#             16528,
#             16540,
#             16527,
#             16544,
#             16503,
#             16545,
#             16510,
#         ],
#     }
#     df_station_name = pd.DataFrame(station_name)
#     return df_station_name


def process_BART_data(combined_gdf, model_run_dir, output_dir, model_BART):
    # Process BART data for different routes and columns
    nodes = read_nodes(model_run_dir)
    # station = station_name()
    bart_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "BART", ["A", "TOD"], "AB_BRDA"
    )
    ebart_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "EBART", ["A", "TOD"], "AB_BRDA"
    )
    oac_boarding = read_dbf_and_groupby_sum(
        combined_gdf, "OAC", ["A", "TOD"], "AB_BRDA"
    )

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
    bart_nodea["Station"] = bart_nodea["Node Name"].map(station_dict)
    # BART_A = pd.merge(BART_A, station, on=["Node"], how="right")
    bart = pd.merge(bart_nodea, bart_nodeb, on=["Node", "TOD"], how="right")

    # Drop rows with specific values
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    bart = bart[~bart["Node Name"].isin(values_to_drop)]

    # Add columns and rearrange columns
    bart["Key"] = bart["Station"] + bart["TOD"]
    bart.columns = [
        "Node",
        "TOD",
        "Boardings",
        "Node Name",
        "Station",
        "Alightings",
        "Key",
    ]
    bart = bart[
        ["Node", "Node Name", "Station", "TOD", "Key", "Boardings", "Alightings"]
    ]

    # Sort and reset index
    bart = bart[["Station", "TOD", "Key", "Boardings", "Alightings"]]
    bart = bart.sort_values(by="Key").reset_index(drop=True)
    bart.to_csv(output_dir / model_BART, index=False)
    return bart


def process_BART_county(
    combined_gdf, model_run_dir, output_dir, model_BART_county, model_BART
):
    BART_county = process_BART_data(combined_gdf, model_run_dir, output_dir, model_BART)
    station_to_county = {
        station: county for county, stations in counties.items() for station in stations
    }

    # Add the 'County' column to the DataFrame
    BART_county["County"] = BART_county["Station"].map(station_to_county)
    BART_county = (
        BART_county.groupby(["County", "TOD"])[["Boardings", "Alightings"]]
        .sum()
        .reset_index()
    )
    BART_county.to_csv(output_dir / model_BART_county, index=False)


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


def BART_Screenline_Concat(combined_gdf, A, B, Screenline):
    # Concatenate the DataFrames
    bart_screenline = process_bart_screenline_data(combined_gdf, A, B)

    # Add the 'Screenline' column with 'Countyline'
    bart_screenline["Screenline"] = Screenline

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


def process_BART_SF(filename, model_run_dir):
    nodes = read_nodes(model_run_dir)
    # df_station_name = station_name()
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
        read_dbf_and_groupby_sum(filename, line, ["A", "B", "TOD"], "AB_VOL")
        for line in lines
    ]

    # Concatenate results
    intra = pd.concat(dfs)

    # Mapping from Node and Station DataFrames
    node_mapping = nodes.set_index("Node")["Node Name"].to_dict()
    # station_mapping = df_station_name.set_index("Node")["Station"].to_dict()

    # Apply mappings
    intra["A_name"] = intra["A"].map(node_mapping)
    intra["B_name"] = intra["B"].map(node_mapping)
    intra["A_station"] = intra["A_name"].map(station_dict)
    intra["B_station"] = intra["B_name"].map(station_dict)

    # Filter out specific values and stations
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    intra = intra[
        ~intra["A_name"].isin(values_to_drop) & ~intra["B_name"].isin(values_to_drop)
    ]
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


def process_BART_Screenline(
    combined_gdf, model_run_dir, output_dir, model_BART_Screenline
):
    transbay_node = [16510, 16511]  # 16510 in, 16511 out
    BART_sl_tb = BART_Screenline_Concat(
        combined_gdf, transbay_node[0], transbay_node[1], "Transbay"
    )
    countyline_node = [16519, 16518]  # 16519n-- in, 16518 --out
    BART_sl_ct = BART_Screenline_Concat(
        combined_gdf, countyline_node[0], countyline_node[1], "Countyline"
    )
    BART_sf = process_BART_SF(combined_gdf, model_run_dir)
    bart_screenline = pd.concat([BART_sl_tb, BART_sl_ct, BART_sf], ignore_index=True)
    bart_screenline.to_csv(output_dir / model_BART_Screenline, index=False)


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    model_run_dir = config["directories"]["model_run"]
    model_BART = config["output"]["model_BART"]
    model_BART_county = config["output"]["model_BART_county"]
    model_BART_Screenline = config["output"]["model_BART_Screenline"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    dbf_file = read_transit_assignments(model_run_dir, time_periods)

    process_BART_Screenline(dbf_file, model_run_dir, output_dir, model_BART_Screenline)
    process_BART_county(dbf_file, model_run_dir, output_dir, model_BART_county)
    process_BART_data(dbf_file, model_run_dir, output_dir, model_BART)
